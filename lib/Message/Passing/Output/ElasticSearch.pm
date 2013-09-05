package Message::Passing::Output::ElasticSearch;
use Moose;
use ElasticSearch;
use AnyEvent;
use Scalar::Util qw/ weaken /;
use MooseX::Types::Moose qw/ ArrayRef Str Bool /;
use Scalar::Util qw/ weaken /;
use Try::Tiny qw/ try catch /;
use aliased 'DateTime' => 'DT';
use MooseX::Types::ISO8601 qw/ ISO8601DateTimeStr /;
use MooseX::Types::DateTime qw/ DateTime /;
use JSON qw/ encode_json /;
use Data::Dumper;
use namespace::autoclean;

our $VERSION = '0.004';
$VERSION = eval $VERSION;

with 'Message::Passing::Role::Output';

has elasticsearch_servers => (
    isa => ArrayRef[Str],
    is => 'ro',
    required => 1,
);

has optimize => (
    isa => 'Bool',
    is => 'ro',
    default => 1,
);

has archive => (
    isa => 'Bool',
    is => 'ro',
    default => 1,
);

has _es => (
    is => 'ro',
    isa => 'ElasticSearch',
    lazy => 1,
    default => sub {
        my $self = shift;
        return ElasticSearch->new(
            transport => "aehttp",
            servers => $self->elasticsearch_servers,
            timeout => 30,
            no_refresh => 1,
 #           trace_calls => 1,
        );
    }
);

has queue => (
    is => 'ro',
    isa => ArrayRef,
    default => sub { [] },
    init_arg => undef,
    lazy => 1,
    clearer => '_clear_queue',
);

has _indexes => (
    isa => 'HashRef',
    lazy => 1,
    is => 'ro',
    default => sub { {} },
    clearer => '_clear_indexes',
);

has verbose => (
    isa => 'Bool',
    is => 'ro',
    default => sub {
        -t STDIN
    },
);

sub consume {
    my ($self, $data) = @_;
     return unless $data;
    my $date;
    if (my $epochtime = delete($data->{epochtime})) {
        $date = DT->from_epoch(epoch => $epochtime);
        delete($data->{date});
    }
    elsif (my $try_date = delete($data->{date})) {
        if (is_ISO8601DateTimeStr($try_date)) {
            $date = to_DateTime($try_date);
        }
    }
    $date ||= DT->from_epoch(epoch => time());
    my $type = $data->{__CLASS__} || 'unknown';
    my $index_name = $self->_index_name_by_dt($date);
    $self->_indexes->{$index_name} = 1;
    my $to_queue = {
        type => $type,
        index => $index_name,
        data => {
            '@timestamp' => to_ISO8601DateTimeStr($date),
            '@tags' => [],
            '@type' => $type,
            '@source_host' => delete($data->{hostname}) || 'none',
            '@message' => exists($data->{message}) ? delete($data->{message}) : encode_json($data),
            '@fields' => $data,
        },
        exists($data->{uuid}) ? ( id => delete($data->{uuid}) ) : (),
    };
    push(@{$self->queue}, $to_queue);
    if (scalar(@{$self->queue}) > 1000) {
        $self->_flush;
    }
}

sub _index_name_by_dt {
    my ($self, $dt) = @_;
    return 'logstash-' . $dt->year . '.' . sprintf("%02d", $dt->month) . '.' . sprintf("%02d", $dt->day);
}

has _am_flushing => (
    isa => Bool,
    is => 'rw',
    default => 0,
);

has _flush_timer => (
    is => 'ro',
    default => sub {
        my $self = shift;
        weaken($self);
        AnyEvent->timer(
            after => 1,
            interval => 1,
            cb => sub { $self->_flush },
        );
    },
);

has _needs_optimize => (
    isa => Bool,
    is => 'rw',
    default => 0,
);

has _optimize_timer => (
    is => 'ro',
    default => sub {
        my $self = shift;
        weaken($self);
        return unless $self->optimize;
        # FIXME!!! This is over-aggressive, you only need to do indexes
        #          when you've finished writing them.
        my $time = 60 * 60; # Every hour
        AnyEvent->timer(
            after => $time,
            interval => $time,
            cb => sub { $self->_needs_optimize(1) },
        );
    },
);

sub _do_optimize {
    my $self = shift;
    weaken($self);
    return unless $self->optimize;
    $self->_am_flushing(1);
    my @indexes = sort keys( %{ $self->_indexes } );
    $self->_clear_indexes;
    $self->_es->optimize_index(
        index => $indexes[0],
        wait_for_merge => 1,
        max_num_segments => 2,
    )->cb(sub {
        warn("Did optimize of " . $indexes[0] . "\n") if $self->verbose;
        $self->_am_flushing(0); $self->_needs_optimize(0) });
}

sub _flush {
    my $self = shift;
    weaken($self);
    return if $self->_am_flushing;
    if ($self->_needs_optimize) {
        return $self->_do_optimize;
    }
    my $queue = $self->queue;
    return unless scalar @$queue;
    $self->_clear_queue;
    $self->_am_flushing(1);
    my $res = $self->_es->bulk_index(
        docs => $queue,
        consistency => 'quorum',
    );
    $res->cb(sub {
        my $res = shift;
        my @indexes = sort keys( %{ $self->_indexes } );
        warn("Indexed " . scalar(@$queue) . " " . join(", ", @indexes) . "\n") if $self->verbose;
        $self->_am_flushing(0);
        foreach my $result (@{ $res->{results} }) {
            if (!$result->{index}->{ok} && !$result->{create}->{ok}) {
                warn "Indexing failure: " . Dumper($result) . "\n";
                last;
            }
        }
    });
}

has _archive_timer => (
    is => 'ro',
    default => sub {
        my $self = shift;
        weaken($self);
        return unless $self->archive;
        my $time = 60 * 60 * 24; # Every day
        AnyEvent->timer(
            after => 60, # delay 1 hour to start first loop
            interval => $time,
            cb => sub { $self->_archive_index() },
        );
    },
);

# _archive_index run 1 time per day to close index older than 7 days and delete
# index older than 30 days
#
sub _archive_index {
    my ($self) = @_;
    return unless $self->archive;

    my $dt = DT->from_epoch(epoch => time());

    my $dt_to_close = $dt->clone->subtract(days => 7);
    my $index_to_close = $self->_index_name_by_dt($dt_to_close);
    $self->_es->close_index(index => $index_to_close)
        ->cb( sub { warn "Close index: $index_to_close \n" if $self->verbose; });

    my $dt_to_delete = $dt->clone->subtract(days => 30);
    my $index_to_delete = $self->_index_name_by_dt($dt_to_delete);
    $self->_es->delete_index(
        index           => $index_to_delete,
        ignore_missing  => 1,
    )->cb( sub { warn "Delete index: $index_to_delete \n" if $self->verbose;});
}


1;

=head1 NAME

Message::Passing::Output::ElasticSearch - output logstash messages into ElasticSearch.

=head1 SYNOPSIS

    message-pass --input STDIN --output ElasticSearch --output_options '{"elasticsearch_servers": ["localhost:9200", "192.0.2.29:9200"]}'

=head1 DESCRIPTION

Currently this output stores messages in the logstash format:

=over

=item index

one index per day in the format logstash-YYYY.MM.dd

=item type

Is taken from the '__CLASS__' attribute or set to 'unknown' if not
defined. If is also stored in the @type field.

=item id

Is takes from the 'uuid' attribute or autogenerated by ElasticSearch.

=item @timestamp

The datetime is taken from the message epochtime or date attribute.

The epochtime attribute is expected in seconds since 1.1.1970 00:00 UTC.

The date attribute is expected to be a string in ISO8601 format.

If both are missing the current timestamp is used. It is used to determine
the index as well as stored in the @timestamp field.

=item @tags

Is set to an empty list.

=item @source_host

Is takes from the message hostname attribute or set to 'none' if not defined.

=item @message

Is taken from the 'message' attribute or the whole message gets JSON encoded.

=item @fields

Is set to all not otherwise processed message attributes.

=back

=head1 METHODS

=head2 elasticsearch_servers

A required attribute for the ElasticSearch server FQDNs or IP addresses including the
port which normally is 9200.

=head2 optimize

An optional boolean attribute, defaults to true. If true, the ElasticSearch
server's C<optimize> method will be called periodically for each index that a
message has been consumed for.

=head2 archive

An optional boolean attribute, defaults to true. If true, a task will run once
a day to delete indexes older than 30 days.

=head2 verbose

A boolean attribute that defaults to true if STDIN is opened to a tty.

=head2 consume ($msg)

Consumes a message, queuing it for consumption by ElasticSearch. The message
has to be a hashref.

=head1 SEE ALSO

=over

=item L<Message::Passing>

=item L<http://logstash.net>

=back

=head1 AUTHOR

Tomas (t0m) Doran <bobtfish@bobtfish.net>

=head1 COPYRIGHT

=head1 LICENSE

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.


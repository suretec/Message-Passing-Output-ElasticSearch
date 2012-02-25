package Log::Stash::Output::ElasticSearch;
use Moose;
use ElasticSearch;
use AnyEvent;
use Scalar::Util qw/ weaken /;
use MooseX::Types::Moose qw/ ArrayRef Str Bool /;
use namespace::autoclean;

our $VERSION = '0.001';
$VERSION = eval $VERSION;

with 'Log::Stash::Mixin::Output';

has elasticsearch_servers => (
    isa => ArrayRef[Str],
    is => 'ro',
    required => 1,
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

sub consume {
    my ($self, $data) = @_;
    return unless $data;
    my $date;
    if ($data->{date}) {
        my @datefields = qw/ year month day /;
        my @timefields = qw/ hour minute second nanosecond /;
        my @datetimefields = (@datefields, @timefields);
        my @fields = map { $_ || 0 } $data->{date} =~ /^(\d{4})-(\d{2})-(\d{2})/;
        $date = join('.', @fields);
    }
    else {
        $date = DateTime->from_epoch(epoch => time()) . "";
    }
    my $type = $data->{__CLASS__} || 'unknown';
    foreach my $name (qw/SYSLOGBASE2 timestamp MONTH MONTHDAY TIME HOUR MINUTE SECOND timestamp8601 YEAR MONTHNUM ISO8601_TIMEZONE SYSLOGFACILITY facility priority" logsource IPORHOST HOSTNAME IP SYSLOGPROG/) {
        $data->{$name} ||= [];
    }
    my $to_queue = {
        type => $type,
        index => 'logstash-' . (ref($date) ? $date->year . '.' . sprintf("%02d", $date->month) . '.' . sprintf("%02d", $date->day) : $date),
        data => {
            '@timestamp' => DateTime->from_epoch(epoch => time()) . "", # FIXME!!
            '@tags' => [],
            '@source' => "lies",
            '@type' => $type,
            '@source_host' => 'moo',
            '@source_path' => 'quack',
            '@message' => exists($data->{message}) ? $data->{message} : 'unknown',
            '@fields' => $data,
        },
        exists($data->{uuid}) ? ( id => $data->{uuid} ) : (),
    };
    #use Data::Dumper; warn Dumper($to_queue);
    push(@{$self->queue}, $to_queue);
    if (scalar(@{$self->queue}) > 1000) {
        $self->_flush;
    }
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

sub _flush {
    my $self = shift;
    my $weak_self = $self;
    return if $self->_am_flushing;
    my $queue = $self->queue;
    return unless scalar @$queue;
    $self->_clear_queue;
    $self->_am_flushing(1);
    my $res = $self->_es->bulk_index($queue);
    weaken($self);
    $res->cb(sub { $self->_am_flushing(0); });
}

1;

=head1 NAME

Log::Stash::Output::ElasticSearch - output logstash messages into ElasticSearch.

=head1 DESCRIPTION

=head1 SEE ALSO

=over

=item L<Log::Stash>

=item L<http://logstash.net>

=back

=head1 AUTHOR

Tomas (t0m) Doran <bobtfish@bobtfish.net>

=head1 SPONSORSHIP

This module exists due to the wonderful people at
L<Suretec Systems|http://www.suretecsystems.com/> who sponsored it's
development.

=head1 COPYRIGHT

Copyright Suretec Systems 2012.

=head1 LICENSE

XXX - TODO


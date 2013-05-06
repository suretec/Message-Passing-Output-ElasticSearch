use strict;
use warnings;
use Test::More;
use Test::Spelling;

add_stopwords(qw(
    ElasticSearch
    Starman
    ZeroMQ
    API
    Affero
    FCGI
    JSON
    Tomas
    Doran
    t0m
    Jorden
    Logstash
    Sissel
    Suretec
    TODO
    STDIN
    STDOUT
    STDERR
    logstash
    FQDNs
    IP
    UTC
    datetime
    epochtime
    hostname
    timestamp
    uuid
));
set_spell_cmd('aspell list -l en');
all_pod_files_spelling_ok();

done_testing();

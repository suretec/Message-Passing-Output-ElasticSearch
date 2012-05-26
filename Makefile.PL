use strict;
use warnings;
use inc::Module::Install;

all_from 'lib/Message/Passing/Output/ElasticSearch.pm';

requires 'Moose';
requires 'namespace::autoclean';
requires 'AnyEvent';
requires 'Try::Tiny';
requires 'ElasticSearch';
requires 'ElasticSearch::Transport::AEHTTP';
requires 'MooseX::Types';
requires 'Message::Passing' => '0.001';

test_requires 'Test::More' => '0.88';

author_requires 'Test::Pod' => '1.14';
author_requires 'Test::NoTabs';
author_requires 'Test::Pod::Coverage' => '1.04';
author_requires 'Pod::Coverage' => '0.19';
author_requires 'Pod::Coverage::TrustPod';
author_requires 'Test::Spelling';

author_tests 't/author';

if ($Module::Install::AUTHOR) {
    system("pod2text lib/Message/Passing/Output/ElasticSearch.pm > README")
        and die $!;
}

WriteAll;


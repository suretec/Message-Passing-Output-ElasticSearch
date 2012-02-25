use strict;
use warnings;
use inc::Module::Install;

all_from 'lib/Log/Stash/Output/ElasticSearch.pm';

requires 'Moose';
requires 'namespace::autoclean';
requires 'AnyEvent';
requires 'Try::Tiny';
requires 'ElasticSearch';
requires 'ElasticSearch::Transport::AEHTTP';
requires 'MooseX::Types';

WriteAll;


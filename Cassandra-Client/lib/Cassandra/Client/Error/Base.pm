package Cassandra::Client::Error::Base;

use 5.010;
use strict;
use warnings;

sub new { my $class= shift; bless { code => -1, message => "An unknown error occurred", @_ }, $class }
use overload '""' => sub { "Error $_[0]{code}: $_[0]{message}" };
sub code { $_[0]{code} }
sub message { $_[0]{message} }
sub is_request_error { $_[0]{request_error} }
sub do_retry { $_[0]{do_retry} }
sub is_timeout { $_[0]{is_timeout} }

1;

package Cassandra::Client::Error::WriteTimeoutException;

use parent 'Cassandra::Client::Error::Base';
use 5.010;
use strict;
use warnings;

sub is_timeout { 1 }
sub cl { $_[0]{cl} }
sub write_type { $_[0]{write_type} }
sub blockfor { $_[0]{blockfor} }
sub received { $_[0]{received} }

1;

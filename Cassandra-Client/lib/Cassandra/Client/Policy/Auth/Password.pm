package Cassandra::Client::Policy::Auth::Password;

use 5.010;
use strict;
use warnings;

use Cassandra::Client::Protocol qw/pack_bytes/;

sub new {
    my ($class, %args)= @_;

    my $username= delete $args{username};
    my $password= delete $args{password};

    return bless {
        username => $username,
        password => $password,
    }, $class;
}

sub begin {
    my ($self)= @_;
    return $self; # We are not a stateful implementation
}

sub evaluate {
    my ($self, $callback, $challenge)= @_;
    my $user= $self->{username};
    my $pass= $self->{password};
    utf8::encode($user) if utf8::is_utf8($user);
    utf8::encode($pass) if utf8::is_utf8($pass);
    return $callback->(undef, pack_bytes("\0$user\0$pass"));
}

sub success {
    my ($self)= @_;
    # Ignored
}

1;

package Cassandra::Client::Pool;
use 5.008;
use strict;
use warnings;

use Scalar::Util 'weaken';
use Cassandra::Client::Util;
use List::Util 'shuffle';

sub new {
    my ($class, %args)= @_;
    my $self= bless {
        client => $args{client},
        options => $args{options},
        metadata => $args{metadata},
        max_connections => $args{options}{max_connections},

        shutdown => 0,
        pool => {},
        count => 0,
        list => [],

        last_id => 0,
        id2ip => {},

        i => 0,

        master_id => undef,
        master_selection => undef,
        network_status => undef,

        connecting => {},
        wait_connect => [],
        datacenter => undef,
    }, $class;
    weaken($self->{client});
    return $self;
}

sub get_one {
    my ($self)= @_;
    return undef unless $self->{count};

    # Round-robin: pick the next one
    return $self->{list}[$self->{i}= (($self->{i}+1) % $self->{count})];
}

sub get_one_cb {
    my ($self, $callback)= @_;

    return $callback->(undef, $self->get_one) if $self->{count};

    if (!%{$self->{connecting}}) {
        $self->connect_if_needed;
    }
    if (!%{$self->{connecting}}) {
        return $callback->("Disconnected: all servers unreachable");
    }

    push @{$self->{wait_connect} ||= []}, $callback;
}

sub remove {
    my ($self, $id)= @_;
    if (!$id) {
        # Probably never got added. Ignore.
        return;
    }

    my $ipaddress= delete $self->{id2ip}{$id};
    if (!$ipaddress) {
        warn 'BUG: Tried to remove an unregistered connection. Probably a bad idea.';
        return;
    }

    my $connection= delete $self->{pool}{$ipaddress};
    if (!$connection) {
        warn 'BUG: Found a registered but unknown connection. This should not happen.';
        return;
    }

    $self->rebuild;

    if ($self->{master_id} && $self->{master_id} == $connection->get_pool_id) {
        $self->{master_id}= undef;
        $self->select_master;
    }

    $self->connect_if_needed;

    return;
}

sub add {
    my ($self, $connection)= @_;

    my $ipaddress= $connection->ip_address;

    if ($self->{pool}{$ipaddress}) {
        warn 'BUG: Duplicate connection for '.$ipaddress.'!';
    }

    my $id= (++($self->{last_id}));
    $connection->set_pool_id($id);
    $self->{pool}{$ipaddress}= $connection;
    $self->{id2ip}{$id}= $ipaddress;

    $self->rebuild;
    $self->select_master unless defined $self->{master_id};

    return;
}

sub rebuild {
    my ($self)= @_;

    $self->{list}= [ values %{$self->{pool}} ];
    $self->{count}= 0+ @{$self->{list}};

    return;
}

sub shutdown {
    my ($self, $callback)= @_;

    $self->{shutdown}= 1;

    my @pool= @{$self->{list}};
    parallel([
        map {
            my $conn= $_;
            sub {
                my $next= shift;
                $conn->shutdown($next, "Shutting down");
            }
        } @pool
    ], $callback);
}

sub warmup {
    my ($self, $callback)= @_;

    # Master selection, warmup, etc
    series([
        sub {
            my ($next)= @_;
            $self->select_master($next);
        },
        sub {
            my ($next)= @_;
            if ($self->{config}{warmup}) {
                $self->connect_if_needed($next);
            } else {
                $self->connect_if_needed();
                return $next->();
            }
        },
    ], $callback);
}

sub connect_if_needed {
    my ($self, $callback)= @_;
    $callback //= sub{};

    return $callback->("Shutting down") if $self->{shutdown};

    my $max_connect= $self->{max_connections} - $self->{count};
    return $callback->() if $max_connect <= 0;

    my @attempts= grep {
               !$self->{pool}{$_}
            && !$self->{connecting}{$_}
            && (!$self->{datacenter} || ($self->{datacenter} eq $self->{network_status}{$_}{data_center}))
        } keys %{$self->{network_status}};
    if (@attempts > $max_connect) {
        @attempts= shuffle @attempts;
        @attempts= @attempts[0..($max_connect-1)];
    }

    parallel([
        map { my $ip= $_; sub {
            my ($next)= @_;

            $self->{connecting}{$ip}= 1;
            my $connection= Cassandra::Client::Connection->new(
                client => $self->{client},
                options => $self->{options},
                host => $ip,
                async_io => $self->{client}{async_io},
                metadata => $self->{metadata},
            );
            $connection->connect(sub {
                my ($error)= @_;
                if (!$error) {
                    $self->add($connection);

                    my $waiters= delete $self->{wait_connect};
                    $_->(undef, $connection) for @$waiters;
                }
                delete $self->{connecting}{$ip};
                return $next->(); # Ignore the error
            });
        } } @attempts
    ], sub {
        if (!%{$self->{connecting}} && !$self->{count} && $self->{wait_connect}) {
            my $waiters= delete $self->{wait_connect};
            # XXX No longer true :-/
            $_->("Unable to connect: no servers reachable") for @$waiters;
        }

        # We can't get errors here, no need to check for them
        $callback->();
    });
}

sub select_master {
    my ($self, $callback)= @_;

    if (!$callback) {
        return if $self->{master_selection};
        return if $self->{shutdown};
        return if defined $self->{master_id};
        return unless $self->{count};
    } else {
        return $callback->('Shutting down') if $self->{shutdown};
        return $callback->() if defined $self->{master_id};
        return $callback->('Not connected') unless $self->{count};
        if ($self->{master_selection}) {
            push @{$self->{master_selection}}, $callback;
            return;
        }
    }

    $self->{master_selection}= [ $callback ? $callback : () ];

    # XXX RETRY (important)

    my $new_master= $self->get_one;
    parallel([
        sub {
            my ($next)= @_;
            $new_master->register_events($next);
        },
        sub {
            my ($next)= @_;
            $new_master->get_network_status(sub {
                my ($error, $status)= @_;
                if ($error) { return $next->($error); }

                $self->{network_status}= $status;

                return $next->();
            });
        }
    ], sub {
        my ($error)= @_;
        $self->{master_id}= $new_master->get_pool_id unless $error;
        my $callbacks= delete $self->{master_selection};
        $_->($error) for @$callbacks;
    });

    return;
}

sub event_added_node {
    my ($self, $ipaddress)= @_;
    $self->refresh_network_status;
    $self->connect_if_needed;
}

sub event_removed_node {
    my ($self, $ipaddress)= @_;
    delete $self->{network_status}{$ipaddress};

    if (my $conn= $self->{pool}{$ipaddress}) {
        $conn->shutdown(undef, "Removed from pool");
    }
}

sub refresh_network_status {
    my ($self)= @_;
    $self->get_one->get_network_status(sub {
        my ($error, $status)= @_;
        $self->{network_status}= $status unless $error;
    });
    #XXX Retries are welcome
}

1;

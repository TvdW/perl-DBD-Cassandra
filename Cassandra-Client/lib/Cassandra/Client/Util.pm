package Cassandra::Client::Util;
use 5.010;
use strict;
use warnings;

use Exporter 'import';
our @EXPORT= ('series', 'parallel');

sub series {
    my $list= shift;
    my $final= shift;
    my $item= shift @$list;

    $item->(sub {
        if (!@$list) {
            return $final->(@_);
        }
        if (my $error= shift) {
            return $final->($error, @_);
        }
        return series($list, $final, @_);
    }, @_);

    return;
}

sub parallel {
    my ($list, $final)= @_;

    if (!@$list) {
        return $final->();
    }

    my $remaining= 0+@$list;
    my @result;
    for my $i (0..$#$list) {
        $list->[$i]->(sub {
            my ($error, $result)= @_;
            if ($error) {
                if ($remaining > 0) {
                    $remaining= 0;
                    $final->($error);
                }
                return;
            }

            $result[$i]= $result;

            $remaining--;
            if ($remaining == 0) {
                $final->(undef, @result);
            }
        });
    }

    return;
}

1;

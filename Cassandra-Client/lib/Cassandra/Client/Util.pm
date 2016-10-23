package Cassandra::Client::Util;
use 5.010;
use strict;
use warnings;

use Exporter 'import';
our @EXPORT= ('series', 'parallel');

sub series {
    my $list= shift;
    my $final= shift;

    my $cb_sub; $cb_sub= sub {
        my $next= shift @$list;
        if ($next && !$_[0]) {
            $_[0]= $cb_sub;
            goto &$next;
        }

        goto &$final;
    };
    (shift @$list)->($cb_sub);

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

package Net::Hadoop::YARN::ResourceManager;

use strict;
use warnings;
use 5.10.0;

use Data::Dumper;
use Moo;
use Ref::Util qw(
    is_ref
    is_arrayref
    is_hashref
);
use Scalar::Util qw(
    refaddr
);

with 'Net::Hadoop::YARN::Roles::Common';

has '+servers' => ( default => sub { ["localhost:8088"] }, );

has '+add_host_key' => ( default => sub { 1 } );

=head1 METHODS

=head2 info

Cluster Information API

=cut

sub active_rm {
    my $self = shift;
    my $opt  = is_hashref $_[0] ? shift @_ : {};
    my $rv;

    foreach my $server ( @{ $self->servers } ) {
        my $info = $self->info({ server => $server });
        my $haState = $info->{haState} || next;
        if ( $haState eq 'ACTIVE' ) {
            $rv = $server;
            last;
        }
    }

    if ( ! $rv ) {
        die sprintf "Failed to locate the active YARN Resource Manager from these hosts: %s",
                        join( q{, }, @{ $self->servers } ),
        ;
    }

    if ( $opt->{hostname_only} ) {
        return +( split m{[:]}xms, $rv )[0];
    }

    return $rv;
}

sub info {
    my $self = shift;
    my $opt  = is_hashref $_[0] ? shift @_ : {};

    my $res = $self->_get(
                    "cluster/info",
                    undef,
                    ( $opt->{server} or () ),
                );

    return $self->_apply_host_key(
                $res,
                $res->{clusterInfo} || $res,
            );
}

=head2 metrics

Cluster Metrics API

=cut

sub metrics {
    my $self = shift;
    my $opt  = is_hashref $_[0] ? shift @_ : {};
    my $res = $self->_get(
                    "cluster/metrics",
                    undef,
                    ( $opt->{server} or () ),
                );

    return $self->_apply_host_key(
                $res,
                $res->{clusterMetrics} || $res,
            );
}

=head2 scheduler

Cluster Scheduler API

=cut

sub scheduler {
    my $self = shift;
    my $res  = $self->_get("cluster/scheduler");
    return $self->_apply_host_key(
                $res,
                $res->{schedulerInfo} || $res,
            );
}

=pod

=head2 apps

Cluster Applications API

=head3 params

params can be either a hash / hashref (options) to get a list, or an appid
(scalar) to get details on a specific app, but not both (no options accepted
when an app id is given)

=over 4

=item state

[deprecated] - state of the application

=item states

applications matching the given application states, specified as a comma-separated list.

=item finalStatus

the final status of the application - reported by the application itself

=item user

user name

=item queue

queue name

=item limit

total number of app objects to be returned

=item startedTimeBegin

applications with start time beginning with this time, specified in ms since epoch

=item startedTimeEnd

applications with start time ending with this time, specified in ms since epoch

=item finishedTimeBegin

applications with finish time beginning with this time, specified in ms since epoch

=item finishedTimeEnd

applications with finish time ending with this time, specified in ms since epoch

=item applicationTypes

applications matching the given application types, specified as a comma-separated list.

=item applicationTags

applications matching any of the given application tags, specified as a comma-separated list.

=back

=cut

sub apps {
    my $self = shift;
    my $app_id;
    my $options;
    if ( @_ == 1 ) {
        if ( !ref $_[0] ) {
            $app_id = shift;
        }
        else {
            $options = shift;
        }
    }
    elsif ( @_ > 1 ) {
        $options = {@_};
    }
    my $res = $self->_get(
        $app_id ? "cluster/apps/$app_id" : ( "cluster/apps", { params => $options } )
    );

    return $self->_apply_host_key(
                $res,
                $res->{apps}{app} || $res->{app} || $res,
            );
}

=head2 attempts

Cluster Application Attempts API : get attempts details on a specific task

=cut

sub appattempts {
    my $self = shift;
    my $app_id = shift or die "No app ID provided";
    my $res = $self->_get( "cluster/apps/$app_id/appattempts" );
    return $res;
}

=head2 appstatistics

Cluster Application Statistics API

=over 4

=item states

states of the applications, specified as a comma-separated list. If states is
not provided, the API will enumerate all application states and return the
counts of them.

=item applicationTypes

types of the applications, specified as a comma- separated list. If
applicationTypes is not provided, the API will count the applications of any
application type. In this case, the response shows * to indicate any
application type. Note that we only support at most one applicationType
temporarily. Otherwise, users will expect an BadRequestException.

=back

=cut

# TODO check all states and add filter (validation)

sub appstatistics {
    my $self = shift;
    my $options;
    if ( @_ == 1 && ref $_[0] ) {
        $options = shift;
    }
    elsif ( @_ > 1 ) {
        $options = {@_};
    }
    my $res = $self->_get(
                    "cluster/appstatistics",
                    ( $options ? {
                        params => $options,
                    } : () ),
                );

    if ($res) {
        return $self->_apply_host_key(
                    $res,
                    $res->{appStatInfo}{statItem} || $res->{statItem},
                );
    }

    return;
}

=head2 nodes

Cluster Nodes API & Cluster Node API: can be either for all nodes, or for a
single one (no options in that case)

=over 4

=item state - the state of the node

=item healthy - true or false

=back

=cut

sub nodes {
    my $self = shift;
    my $node_id;
    my $options;
    if ( @_ == 1 ) {
        if ( !ref $_[0] ) {
            $node_id = shift;
        }
        else {
            $options = shift;
        }
    }
    elsif ( @_ > 1 ) {
        $options = {@_};
    }
    my $res = $self->_get(
                    $node_id ? "cluster/nodes/$node_id"
                             :  (
                                    "cluster/nodes",
                                    { params => $options },
                                )
                );


    return $self->_apply_host_key(
                $res,
                $res->{nodes}{node} || $res->{node} || $res,
            );

}

=head2 Cluster Writeable APIs

Currently in alpha, not implemented in this class

=over 4

=item Cluster New Application API

=item Cluster Applications API(Submit Application)

=item Cluster Application State API

=item Cluster Delegation Tokens API

=back

=cut

sub _apply_host_key {
    my $self = shift;
    my $res  = shift;
    my $rv   = shift;


    if (   is_ref( $res )
        && is_ref( $rv )
        && ( refaddr $res eq refaddr $rv )
    ) {
        return $rv;
    }

    my $host_key  = $self->host_key;
    my $this_host = $res->{ $host_key };

    if ( is_hashref $rv ) {
        $rv->{ $host_key } = $this_host;
    }
    elsif ( is_arrayref $rv ) {
        foreach my $e ( @{ $rv } ) {
            $e->{ $host_key } = $this_host;
        }
    }
    else {
        die "Got unknown data: $rv";
    }

    return $rv;
}

1;

__END__

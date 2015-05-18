package Net::Hadoop::YARN::NodeManager;

use strict;
use warnings;
use 5.10.0;
use Moo;
use Data::Dumper;

with 'Net::Hadoop::YARN::Roles::Common';

has '+servers' => ( default => sub { ["localhost:50060"] }, );

=head1 METHODS

=head2 info

NodeManager Information API

=cut

sub info {
    my $self = shift;
    my $res = $self->_get("node/info");
    return $res->{nodeInfo} || $res;
}

=head2 apps

=head2 app

Applications API & Application API

parameters are either a hash(/ref) or an application ID

=item state

application state 

=item user

user name

=back
=cut

sub app {
    my $self = shift;
    return $self->apps(@_);
}

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
        $app_id ? "node/apps/$app_id" : ( "node/apps", { params => $options } )
    );
    return $res->{app} || $res;
}

=head2 containers

=head2 container

Containers API

pass a container ID to get information on a specific one, otherwise get the full list

=cut

sub container {
    my $self = shift;
    return $self->containers(@_);
}

sub containers {
    my $self = shift;
    my $container_id = shift;
    my $res = $self->_get( "node/containers" . ($container_id ? "/$container_id" : "") );
    return $res->{container} || $res;
}

1;


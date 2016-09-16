package Net::Hadoop::YARN::DataNode::JMX;
use 5.10.0;
use strict;
use warnings;

use Moo;

my $RE_PATH_KEY = qr{ Class[.]?Path }xmsi;

sub java_runtime {
    my $self = shift;
    my $run  = $self->collect( ['java.lang:type=Runtime'] ) || die "failed to collect Java runtime stats";
    my $bean = $run->{java}{lang}{type}{Runtime}{beans}[0]  || die "failed to collect Java runtime stats*";
    my $sys = $bean->{SystemProperties} = {
        map { $_->{key} => $_->{value} }
        @{ $bean->{SystemProperties } }
    };

    my $sep = quotemeta $sys->{'path.separator'};

    foreach my $path ( grep { $_ =~ $RE_PATH_KEY } keys %{ $sys } ) {
        $sys->{ $path } = [ split $sep, $sys->{ $path } ];
    }

    foreach my $path ( grep { $_ =~ $RE_PATH_KEY } keys %{ $bean } ) {
        $bean->{ $path } = [ split $sep, $bean->{ $path } ];
    }

    return $bean;
}

1;

__END__

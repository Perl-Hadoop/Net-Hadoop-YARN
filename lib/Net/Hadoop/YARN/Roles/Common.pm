package Net::Hadoop::YARN::Roles::Common;

use strict;
use warnings;
use 5.10.0;

use Moo::Role;

use URI;
use JSON;
use LWP::UserAgent;
use HTTP::Request;
use Data::Dumper;
use Socket;
use Regexp::Common qw /net/;
use XML::LibXML::Simple;

has _json => (
    is      => 'rw',
    default => sub {
        require JSON::XS;
        return JSON::XS->new->pretty(1)->canonical(1);
    },
    isa => sub {
        die "Not a JSON object"
            if eval { ref( $_[0] ) !~ /^JSON(:|$)/ || !$_[0]->can('decode') };
    },
    lazy => 1,
);

has debug => (
    is => 'rw',
    default => sub { $ENV{NET_HADOOP_YARN_DEBUG} || 0 },
    isa => sub { die 'debug should be an integer' if $_[0] !~ /^[0-9]$/ },
    lazy => 1,
);

has ua => (
    is      => 'rw',
    default => sub { return LWP::UserAgent->new( env_proxy => 0, timeout => $_[0]->timeout ) },
    isa     => sub {
        die "'ua' isn't a LWP::UserAgent"
            if !eval { $_[0]->isa("LWP::UserAgent") };
    },
    lazy => 1,
);

has timeout => (
    is      => 'rw',
    default => sub {30},
    isa     => sub { die "timeout must be an integer" if $_[0] !~ /^[0-9]+$/ || $_[0] <= 0 },
    lazy    => 1,
);

has servers => (
    is  => 'rw',
    isa => sub {
        die "Incorrect server list" if ! _check_servers(@_);
    },
    lazy => 1,
);

sub _check_host {
    my $host = shift;
    return !!( eval { inet_aton($host) }
        || $host =~ $RE{net}{IPv4}
        || $host =~ $RE{net}{IPv6} );
}

sub _check_servers {
    for my $server (@{+shift}) {
        my ($host, $port) = split /:/, $server, 2;
        die "server $server bad host" if ! _check_host($host);
        die "server $server bad port" if ($port !~ /^[0-9]+$/ || $port < 1 || $port > 65535);
    }
    return 1;
}

sub _mk_uri {
    my $self = shift;
    my ( $server, $path, $params ) = @_;
    my $uri = $server . "/ws/v1/" . $path;
    $uri =~ s#//+#/#g;
    $uri = URI->new("http://" . $uri);
    if ($params) {
        $uri->query_form($params);
    }
    return $uri;
}

# http://hadoop.apache.org/docs/r2.2.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html

sub _get {
    shift->_request( 'GET', @_ );
}

sub _put {
    shift->_request( 'PUT', @_ );
}

sub _post {
    shift->_request( 'POST', @_ );
}

sub _request {
    my $self = shift;
    my $maxtries = @{$self->servers};

    my ($eval_error, $ret);

    TRY: for (1..$maxtries) {

        $ret = eval {
            $eval_error = undef;
            my ( $method, $path, $extra ) = @_;

            my $uri = $self->_mk_uri( $self->servers()->[0], $path, $method eq 'GET' ? $extra->{params} : () );
            print "====> $uri\n" if $self->debug;
            my $req = HTTP::Request->new( uc($method), $uri );
            $req->header( "Accept-Encoding", "gzip" );
            #$req->header( "Accept", "application/json" );
            $req->header( "Accept", "application/xml" );

            my $response = $self->ua->request($req);

            if ( $response->code == 500 ) {
                die "Bad request: $uri";
            }
            my $content = $response->decoded_content;
            #print "$content\n" if $self->debug;
            my $decode_error;

            # found out the json support is buggy at least in the scheduler
            # info (overwrites child queues instead of making a list), revert
            # to XML (see YARN-2336)
            my $res = eval {
                ( $content || '' ) =~ /^\s*</ or die "doesn't look like XML";
                XMLin(
                    $content,
                    KeyAttr    => [],
                    ForceArray => [qw(task job taskAttempt jobAttempt container app appAttempt counterGroup)],
                    KeepRoot   => 0
                );
            }    # $self->_json->decode($content)
                or do { $decode_error = $@ };
            if ($decode_error) {

                # when redirected to the history server, a bug present in hadoop 2.5.1
                # sends to an HTML page, ignoring the Accept-Type header
                if ($response->redirects) {
                    die "server response wasn't valid (possibly buggy redirect to HTML instead of JSON or XML)";
                }
                die "server response wasn't valid JSON or XML";
            }
            print Dumper $res if $self->debug;
            if ( $response->is_success ) {
                return $res; # get out of this eval
            }
            else {
                die
                    "$res->{RemoteException}{message} "
                    . "($res->{RemoteException}{exception} in $res->{RemoteException}{javaClassName}) "
                    . "for URI: $uri";
            }
        } or do {

            # store the error for later; will be displayed if this is the last
            # iteration. also use the next server in the list in case of retry,
            # or reset the list for the next call (we went a full circle)
            $eval_error = $@;
            push @{$self->servers}, shift @{$self->servers};
        };

        last TRY if $ret;
    } # retry as many times as there are servers

    die $eval_error if $eval_error; # still here? that's a fail!
    return $ret;
}

1;


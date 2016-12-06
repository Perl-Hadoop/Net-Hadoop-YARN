package Net::Hadoop::YARN::ApplicationMaster;

use strict;
use warnings;
use 5.10.0;
use Moo;

use Clone ();
use List::MoreUtils qw( uniq );

use Constant::FromGlobal DEBUG => { int => 1, default => 0, env => 1 };
use HTML::PullParser;
use Scalar::Util qw( blessed );

use Net::Hadoop::YARN::HistoryServer;

with 'Net::Hadoop::YARN::Roles::AppMasterHistoryServer';
with 'Net::Hadoop::YARN::Roles::Common';

has '+servers' => (
    default => sub {["localhost:8088"]},
);

has history_object => (
    is  => 'rw',
    isa => sub {
        my $o = shift || return;
        if ( ! blessed $o || ! $o->isa('Net::Hadoop::YARN::HistoryServer') ) {
            die "$o is not a Net::Hadoop::YARN::HistoryServer";
        }
    },
    lazy    => 1,
    default => sub { },
);

my $PREFIX = '_' x 4;

#<<<
my $methods_urls = {
    jobs                => ['/proxy/{appid}/ws/v1/mapreduce/jobs',                                                      'job'                     ],
    job                 => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}',                                              ''                        ],
    jobconf             => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/conf',                                         ''                        ],
    jobcounters         => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/counters',                                     'counterGroup'            ],
    jobattempts         => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/jobattempts',                                  'jobAttempt'              ],
    _get_tasks          => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks',                                        'task'                    ],
    task                => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}',                               ''                        ],
    taskcounters        => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/counters',                      'taskCounterGroup'        ],
    taskattempts        => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts',                      'taskAttempt'             ],
    taskattempt         => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}',          ''                        ],
    taskattemptcounters => ['/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters', 'taskAttemptCounterGroup' ],
};
#>>>

# For each of the keys, at startup:
# - make a method, adding the path
# - pass the path and variables to a validation and substitution engine
# - execute the request
# - return the proper fragment of the JSON tree

_mk_subs($methods_urls, { prefix => $PREFIX } );

my %app_to_hist = (
    jobs => [ job => qr{ \A job_[0-9]+ }xms ],
);

foreach my $name ( keys %{ $methods_urls } ) {
    my $base = $PREFIX . $name;
    my $hist_method = $app_to_hist{ $name } || [ $name ];
    no strict qw( refs );
    *{ $name } = sub {
        my $self = shift;
        my $args = Clone::clone( \@_ );
        my @rv;
        eval {
            @rv = $self->$base( @_ );
            1;
        } or do {
            my $eval_error = $@ || 'Zombie error';
            if (
                $eval_error =~ m{ \Qserver response wasn't valid (possibly buggy redirect to HTML instead of JSON or XML\E }xms
                && $self->history_object
            ) {
                if ( DEBUG ) {
                    printf STDERR "Received HTML from the API. ",
                                  "I will now attempt to collect the information from the history server\n";
                    printf STDERR "The error was: %s\n", $eval_error
                        if DEBUG > 1;
                }

                my $really_gone = $eval_error =~ m{
                    Application .+? \Qcould not be found, please try the history server\E
                }xms;

                if ( $really_gone ) {
                    my @orig =  @{ $args };
                    my($hmethod, $hregex) = @{ $hist_method };
                    @rv = $self->history_object->$hmethod(
                                map {
                                    (my $c = $_) =~ s{ \bapplication_ }{job_}xms;
                                    $c;
                                } @orig
                            );
                }
                else {
                    eval {
                        my(undef, $html) = split m{\Q<!DOCTYPE\E}xms, $eval_error, 2;
                        $html = '<!DOCTYPE' . $html;
                        my $parser = HTML::PullParser->new(
                                        doc         => \$html,
                                        start       => 'event, tagname, @attr',
                                        report_tags => [qw( a )],
                                    ) || die "Can't parse HTML received from the API: $!";
                        my %link;
                        while ( my $token = $parser->get_token ) {
                            next if $token->[0] ne 'start';
                            my($type, $tag, %attr) = @{ $token };
                            my $link = $attr{href} || next;
                            $link{ $link }++;
                        }
                        my %id;
                        for my $link ( keys %link ) {
                            $id{ $_ }++ for $self->_extract_valid_params( $link );
                        }
                        my @ids = keys %id;
                        my($hmethod, $hregex) = @{ $hist_method };
                        my($id) = $hregex ? grep { $_ =~ $hregex } @ids : ();
                        @rv = $self->history_object->$hmethod( $id );
                        1;
                    } or do {
                        my $eval_error_hist = $@ || 'Zombie error';
                        die "Received HTML from the API and attempting to map that to a historical job failed: $eval_error\n$eval_error_hist\n";
                    };
                }
            }
            else {
                die $eval_error;
            }
        };
        return @rv;
    };
}

sub info {
    my $self = shift;
    $self->mapreduce(@_);
}

sub mapreduce {
    my $self   = shift;
    my $app_id = shift;
    my $res    = $self->_get("{appid}/ws/v1/mapreduce/info");
    return $res->{info};
}

sub tasks {
    my $self = shift;
    $self->_get_tasks(@_);
}

1;

__END__

=head1 NAME

Net::Hadoop::YARN::ApplicationMaster

Implementation of the REST API described in
L<http://hadoop.apache.org/docs/r2.5.1/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapredAppMasterRest.html>

=head1 METHODS

Most of the methods are described in
L<Net::Hadoop::YARN::Roles::AppMasterHistoryServer> as both the Application Master
and History Server implement them. Please refer to the role for a full list and
arguments.

=head2 mapreduce

=head2 info

Mapreduce Application Master Info API

http://<proxy http address:port>/proxy/{appid}/ws/v1/mapreduce/info

=head2 tasks

Tasks API

Takes application ID as a first argument, plus an optional options hashref:

=over 4

=item type

type of task, valid values are m or r.  m for map task or r for reduce task.

=back

=cut

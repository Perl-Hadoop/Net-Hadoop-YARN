package Net::Hadoop::YARN::HistoryServer;

use strict;
use warnings;
use 5.10.0;
use Moo;

with 'Net::Hadoop::YARN::Roles::AppMasterHistoryServer';
with 'Net::Hadoop::YARN::Roles::Common';

#<<<
my $methods_urls = {
    _get_jobs           => ['/ws/v1/history/mapreduce/jobs',                                                      'job'                     ],
    job                 => ['/ws/v1/history/mapreduce/jobs/{jobid}',                                              ''                        ],
    jobconf             => ['/ws/v1/history/mapreduce/jobs/{jobid}/conf',                                         ''                        ],
    jobcounters         => ['/ws/v1/history/mapreduce/jobs/{jobid}/counters',                                     'counterGroup'            ],
    jobattempts         => ['/ws/v1/history/mapreduce/jobs/{jobid}/jobattempts',                                  'jobAttempt'              ],
    _get_tasks          => ['/ws/v1/history/mapreduce/jobs/{jobid}/tasks',                                        'task'                    ],
    task                => ['/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}',                               ''                        ],
    taskcounters        => ['/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/counters',                      'taskCounterGroup'        ],
    taskattempts        => ['/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts',                      'taskAttempt'             ],
    _get_taskattempt    => ['/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}',          ''                        ],
    taskattemptcounters => ['/ws/v1/history/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters', 'taskAttemptCounterGroup' ],
};
#>>>

# For each of the keys:
# - make a method, adding the path
# - pass the path and variables to a validation and substitution engine
# - execute the request
# - return the proper fragment of the JSON tree

_mk_subs($methods_urls);

has '+servers' => (
    default => sub { ["localhost:19888"] },    # same as resource manager by default
);

=head1 NAME

Net::Hadoop::YARN::HistoryServer

Implementation of the REST API described in
L<http://hadoop.apache.org/docs/r2.5.1/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/HistoryServerRest.html#Job_API>

=head1 METHODS

Most of the methods are described in
L<Net::Hadoop::YARN::Roles::AppMasterHistoryServer> as both the Application Master
and History Server implement them. Please refer to the role for a full list and
arguments.

=head2 info

History Server Info API

http://<history server http address:port>/ws/v1/history/info

=cut

sub info {
    my $self = shift;
    my $res = $self->_get("info");
    return $res->{info};
}

=head2 jobs

History Server jobs API - get a listing of finished jobs

An optional hashref can be passed to the method in order to restrict the search

=over 4

=item user

user name

=item state

the job state

=item queue

queue name

=item limit

total number of app objects to be returned

=item startedTimeBegin

jobs with start time beginning with this time, specified in ms since epoch

=item startedTimeEnd

jobs with start time ending with this time, specified in ms since epoch

=item finishedTimeBegin

jobs with finish time beginning with this time, specified in ms since epoch

=item finishedTimeEnd

jobs with finish time ending with this time, specified in ms since epoch

=back

=cut

sub jobs {
    my $self = shift;
    $self->_get_jobs(@_);
}

=head2 tasks

Tasks API

=over 4

=item type

type of task, valid values are m or r.  m for map task or r for reduce task.

=back

=cut

sub tasks {
    my $self = shift;
    #use Data::Dumper; print Dumper \@_;
    $self->_get_tasks(@_);
}

sub taskattempt {
    my $self = shift;
    my $attempt = $self->_get_taskattempt(@_);
}

1;
__END__

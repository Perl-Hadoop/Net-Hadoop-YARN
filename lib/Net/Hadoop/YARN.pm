package Net::Hadoop::YARN;

# ABSTRACT: Communicate with Apache Hadoop NextGen MapReduce (YARN) 

use strict;
use warnings;
use 5.10.0;

use Constant::FromGlobal DEBUG => { int => 1, default => 0, env => 1 };
use Net::Hadoop::YARN::ResourceManager;
use Net::Hadoop::YARN::NodeManager;
use Net::Hadoop::YARN::ApplicationMaster;
use Net::Hadoop::YARN::HistoryServer;

1;

#! /usr/bin/env perl

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


use POSIX qw(strftime);
use Cwd qw();

if(scalar(@ARGV) != 4 && scalar(@ARGV) != 2){
  print "Usage : tserver-agitator.pl <min sleep before kill in minutes>[:max sleep before kill in minutes] <min sleep before tup in minutes>[:max sleep before tup in minutes] [<min kill> <max kill>]\n";
  exit(1);
}

$cwd=Cwd::cwd();
$ACCUMULO_HOME=$cwd . '/../../..';

print "ACCUMULO_HOME=$ACCUMULO_HOME\n";

@sleeprange1 = split(/:/, $ARGV[0]);
$sleep1 = $sleeprange1[0];

@sleeprange2 = split(/:/, $ARGV[1]);
$sleep2 = $sleeprange2[0];

if (scalar(@sleeprange1) > 1) {
  $sleep1max = $sleeprange1[1] + 1;
} else {
  $sleep1max = $sleep1;
}

if ($sleep1 > $sleep1max) {
  die("sleep1 > sleep1max $sleep1 > $sleep1max");
}

if (scalar(@sleeprange2) > 1) {
  $sleep2max = $sleeprange2[1] + 1;
} else {
  $sleep2max = $sleep2;
}

if($sleep2 > $sleep2max){
  die("sleep2 > sleep2max $sleep2 > $sleep2max");
}

if(defined $ENV{'ACCUMULO_CONF_DIR'}){
  $ACCUMULO_CONF_DIR = $ENV{'ACCUMULO_CONF_DIR'};
}else{
  $ACCUMULO_CONF_DIR = $ACCUMULO_HOME . '/conf';
}

if(scalar(@ARGV) == 4){
  $minKill = $ARGV[2];
  $maxKill = $ARGV[3];
}else{
  $minKill = 1;
  $maxKill = 1;
}

if($minKill > $maxKill){
  die("minKill > maxKill $minKill > $maxKill");
}

@slavesRaw = `cat $ACCUMULO_CONF_DIR/slaves`;
chomp(@slavesRaw);

for $slave (@slavesRaw){
  if($slave eq "" || substr($slave,0,1) eq "#"){
    next;
  }

  push(@slaves, $slave);
}


if(scalar(@slaves) < $maxKill){
  print STDERR "WARN setting maxKill to ".scalar(@slaves)."\n";
  $maxKill = scalar(@slaves);
}

if ($minKill > $maxKill){
  print STDERR "WARN setting minKill to equal maxKill\n";
  $minKill = $maxKill;
}

while(1){

  $numToKill = int(rand($maxKill - $minKill + 1)) + $minKill;
  %killed = {};
  $server = "";

  for($i = 0; $i < $numToKill; $i++){
    while($server eq "" || $killed{$server} != undef){
      $index = int(rand(scalar(@slaves)));
      $server = $slaves[$index];
    }

    $killed{$server} = 1;

    $t = strftime "%Y%m%d %H:%M:%S", localtime;

    print STDERR "$t Killing tserver on $server\n";
    # We're the accumulo user, just run the commandj
    system("$ACCUMULO_HOME/bin/stop-server.sh $server 'accumulo-start.jar' tserver KILL");
  }

  $nextsleep2 = int(rand($sleep2max - $sleep2)) + $sleep2;
  sleep($nextsleep2 * 60);
  $t = strftime "%Y%m%d %H:%M:%S", localtime;
  print STDERR "$t Running tup\n";
  # restart the as them as the accumulo user
  system("$ACCUMULO_HOME/bin/tup.sh");

  $nextsleep1 = int(rand($sleep1max - $sleep1)) + $sleep1;
  sleep($nextsleep1 * 60);
}


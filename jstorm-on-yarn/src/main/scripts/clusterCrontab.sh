#!/usr/bin/expect

## Access CLI
set loginUser "jian.feng"
set loginPassword ""
set mgmtServerAddress manager_host

## Expect Parameters
set timeout 20
set successMsg "Status: Success"
set failureMsg "Status: Failure"

spawn pgm -A -b -p 10 -f rack.txt "sudo chmod -R 777 /home/yarn  && find /home/yarn/jstorm_logs/*  -name '*.hprof'  ! -type d  -mmin +90 | xargs rm -rf && find /home/yarn/jstorm_logs/* ! -type d  -mmin +2400 | xargs rm -rf"
expect_after eof {exit 0}

set timeout 10

##interact with SSH
##expect "yes/no" {send "yes\r"}
expect "Password:" {send "$loginPassword\r"}
puts "\n## Starting Generated OVMCLI Script... ##\n"
set timeout 600

expect "OVM> "
send "set OutputMode=Verbose\r"
expect $successMsg {} \
    timeout { puts "\n\nTest Failure: \n\r"; exit}

expect "OVM> "
  send  "list Server\r"
  expect $successMsg {} \
   timeout { puts "\n\nScript Failure: \n\r"; exit}
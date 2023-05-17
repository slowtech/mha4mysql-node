#!/usr/bin/env perl

#  Copyright (C) 2011 DeNA Co.,Ltd.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#  Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

package MHA::NodeUtil;

use strict;
use warnings FATAL => 'all';

use Carp qw(croak); # croak 是 Carp 模块中的一个函数，用于生成带有堆栈追踪的致命错误信息。当出现错误并且程序无法继续执行时，可以使用 croak 函数来报告错误并退出程序。
use MHA::NodeConst;
use File::Path;
use Errno();
use Socket qw(NI_NUMERICHOST getaddrinfo getnameinfo);

sub create_dir_if($) {
  my $dir = shift;
  if ( !-d $dir ) {
    eval {
      print "Creating directory $dir.. ";
      mkpath($dir);
      print "done.\n";
    };
    if ($@) { # 当在Perl中使用eval块时，如果eval块中的代码抛出了异常，错误信息将被存储在$@变量中。如果eval块中的代码没有抛出异常，$@将保持未定义（undefined）。
      my $e = $@;
      undef $@; # 清除 $@ 变量，以便后续代码中的 croak 函数不会将异常再次抛出。
      if ( -d $dir ) {
        print "ok. already exists.\n";
      }
      else {
        croak "failed to create dir:$dir:$e";
      }
    }
  }
}

# Compare file checksum between local and remote host
sub compare_checksum {
  my $local_file  = shift;
  my $remote_path = shift;
  my $ssh_user    = shift;
  my $ssh_host    = shift;
  my $ssh_port    = shift;
  $ssh_port = 22 unless ($ssh_port);

  my $local_md5 = `md5sum $local_file`;
  return 1 if ($?); # 在Perl中，特殊变量 $? 是一个用于获取上一个执行的子进程的退出状态的变量。它用于检查子进程的执行结果，包括退出码和信号信息。如果执行正常，$? 的值将为 0。
  chomp($local_md5);
  $local_md5 = substr( $local_md5, 0, 32 );
  my $ssh_user_host = $ssh_user . '@' . $ssh_host;
  my $remote_md5 =
`ssh $MHA::NodeConst::SSH_OPT_ALIVE -p $ssh_port $ssh_user_host \"md5sum $remote_path\"`;
  return 1 if ($?);
  chomp($remote_md5);
  $remote_md5 = substr( $remote_md5, 0, 32 );
  return 2 if ( $local_md5 ne $remote_md5 );
  return 0;
}

sub file_copy {
  my $to_remote   = shift;
  my $local_file  = shift;
  my $remote_file = shift;
  my $ssh_user    = shift;
  my $ssh_host    = shift;
  my $log_output  = shift;
  my $ssh_port    = shift;
  $ssh_port = 22 unless ($ssh_port);
  my $dsn_host = $ssh_host =~ m{:} ? '[' . $ssh_host . ']' : $ssh_host;
  my $ssh_user_host = $ssh_user . '@' . $dsn_host;
  my ( $from, $to );
  if ($to_remote) {
    $from = $local_file;
    $to   = "$ssh_user_host:$remote_file";
  }
  else {
    $to   = $local_file;
    $from = "$ssh_user_host:$remote_file";
  }

  my $max_retries = 3;
  my $retry_count = 0;
  my $copy_fail   = 1;
  my $copy_command =
    "scp $MHA::NodeConst::SSH_OPT_ALIVE -P $ssh_port $from $to";
  if ($log_output) {
    $copy_command .= " >> $log_output 2>&1";
  }

  while ( $retry_count < $max_retries ) { # 最多重试三次
    if (
      system($copy_command)
      || compare_checksum(
        $local_file, $remote_file, $ssh_user, $ssh_host, $ssh_port
      )
      )
    {
      my $msg = "Failed copy or checksum. Retrying..";
      if ($log_output) {
        system("echo $msg >> $log_output 2>&1");
      }
      else {
        print "$msg\n";
      }
      $retry_count++;
    }
    else {
      $copy_fail = 0;
      last;
    }
  }
  return $copy_fail;
}

sub system_rc($) { # 将返回的退出状态码分解为高位和低位的值，以便在需要时进行进一步处理或分析
  my $rc   = shift;
  my $high = $rc >> 8;
  my $low  = $rc & 255;
  return ( $high, $low );
}
# open( my $out, ">", $file )：使用 open 函数打开一个文件 $file 用于写入，并将文件句柄存储在变量 $out 中。">" 表示以写入模式打开文件。
sub create_file_if {
  my $file = shift;
  if ( $file && ( !-f $file ) ) {
    open( my $out, ">", $file ) or croak "$!:$file"; # $! 是一个特殊变量，它包含了最近一次系统调用的错误信息
    close($out);
  }
}

sub drop_file_if($) {
  my $file = shift;
  if ( $file && -f $file ) {
    unlink $file or croak "$!:$file";
  }
}

sub get_ip {
  my $host = shift;
  my ( $err, @bin_addr_host, $addr_host );
  if ( defined($host) ) {
    ( $err, @bin_addr_host ) = getaddrinfo($host);
    croak "Failed to get IP address on host $host: $err\n" if $err;
    # 使用 getnameinfo 函数和 NI_NUMERICHOST 选项从 @bin_addr_host 数组的第一个元素中提取 IP 地址。
    # We take the first ip address that is returned by getaddrinfo
    ( $err, $addr_host ) = getnameinfo($bin_addr_host[0]->{addr}, NI_NUMERICHOST);
    croak "Failed to convert IP address for host $host: $err\n" if $err;

    # for IPv6 (and it works with IPv4 and hostnames as well):
    # - DBD-MySQL expects [::] format
    # - scp requires [::] format
    # - ssh requires :: format
    # - mysql tools require :: format
    # The code in MHA is expected to use [] when it is running scp
    # when it connects with DBD-MySQL

    return $addr_host;
  }
  return;
}

sub current_time() {
  my ( $sec, $min, $hour, $mday, $mon, $year ) = localtime();
  $mon  += 1;
  $year += 1900;
  my $val = sprintf( "%d-%02d-%02d %02d:%02d:%02d",
    $year, $mon, $mday, $hour, $min, $sec );
  return $val;
}

sub check_manager_version {
  my $manager_version = shift;
  if ( $manager_version < $MHA::NodeConst::MGR_MIN_VERSION ) {
    croak
"MHA Manager version is $manager_version, but must be $MHA::NodeConst::MGR_MIN_VERSION or higher.\n";
  }
}

sub parse_mysql_version($) {
  my $str = shift;
  my $result = sprintf( '%03d%03d%03d', $str =~ m/(\d+)/g );
  return $result;
}

sub parse_mysql_major_version($) {
  my $str = shift;
  $str =~ s/\.[^.]+$//;
  my $result = sprintf( '%03d%03d', $str =~ m/(\d+)/g );
  return $result;
}

sub mysql_version_ge {
  my ( $my_version, $target_version ) = @_;
  my $result =
    parse_mysql_version($my_version) ge parse_mysql_version($target_version)
    ? 1
    : 0;
  return $result;
}

my @shell_escape_chars = (
  '"', '!', '#', '&', ';', '`', '|',    '*',
  '?', '~', '<', '>', '^', '(', ')',    '[',
  ']', '{', '}', '$', ',', ' ', '\x0A', '\xFF'
);
# 取消 shell 中字符串中的转义字符，以便正确地处理或执行相关的 shell 命令
sub unescape_for_shell {
  my $str = shift;
  if ( !index( $str, '\\\\' ) ) {
    return $str;
  }
  foreach my $c (@shell_escape_chars) {
    my $x       = quotemeta($c);
    my $pattern = "\\\\(" . $x . ")";
    $str =~ s/$pattern/$1/g;
  }
  return $str;
}

sub escape_for_shell {
  my $str = shift;
  my $ret = "";
  foreach my $c ( split //, $str ) {
    my $x      = $c;
    my $escape = 0;
    foreach my $e (@shell_escape_chars) {
      if ( $e eq $x ) {
        $escape = 1;
        last;
      }
    }
    if ( $x eq "'" ) {
      $x =~ s/'/'\\''/;
    }
    if ( $x eq "\\" ) {
      $x = "\\\\";
    }
    if ($escape) {
      $x = "\\" . $x;
    }
    $ret .= "$x";
  }
  $ret = "'" . $ret . "'";
  return $ret;
}
# 对字符串进行 shell 转义
sub escape_for_mysql_command {
  my $str = shift;
  my $ret = "";
  foreach my $c ( split //, $str ) {
    my $x = $c;
    if ( $x eq "'" ) {
      $x =~ s/'/'\\''/;
    }
    $ret .= $x;
  }
  $ret = "'" . $ret . "'";
  return $ret;
}
# 可以根据给定的客户端二进制文件名、二进制文件路径和库文件路径生成相应的客户端命令行前缀，包括必要的环境变量设置和路径转义。
sub client_cli_prefix {
  my ( $exe, $bindir, $libdir ) = @_;
  croak "unexpected client binary $exe\n" unless $exe =~ /^mysql(?:binlog)?$/;
  my %env = ( LD_LIBRARY_PATH => $libdir );
  my $cli = $exe;
  if ($bindir) {
    if ( ref $bindir eq "ARRAY" ) {
      $env{'PATH'} = $bindir;
    }
    elsif ( ref $bindir eq "" ) {
      $cli = escape_for_shell("$bindir/$exe");
    }
  }
  for my $k ( keys %env ) {
    if ( my $v = $env{$k} ) {
      my @dirs = ref $v eq "ARRAY" ? @{$v} : ( ref $v eq "" ? ($v) : () );
      @dirs = grep { $_ && !/:/ } @dirs;
      if (@dirs) {
        $cli = "$k="
          . join( ":", ( map { escape_for_shell($_) } @dirs ), "\$$k" )
          . " $cli";
      }
    }
  }

  # $cli .= " --no-defaults";
  return $cli;
}

1;

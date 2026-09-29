#
# Patches the length of a GTID tag stored in a binary log file on disk:
# finds the first event of the given type that contains the tag (encoded as
# a one-byte variable-length integer followed by the tag characters) and
# replaces the encoded length, recalculating the event checksum when the
# event has a footer. The event size is not changed.
#
# The binary log file must be closed (the server must not be writing to it).
#
# Usage:
#   --let PATCHED_BINLOG_PATH = /path/to/binlog.000001
#   --let PATCHED_EVENT_CODE = 42 (GTID_TAGGED_LOG) | 35 (PREVIOUS_GTIDS_LOG)
#   --let PATCHED_TAG = my_tag
#   --let PATCHED_TAG_LENGTH = 40 (must be < 128)
#   --let PATCHED_BINLOG_HAS_CHECKSUMS = 1 | 0
#   --source ../include/patch_tag_length_in_binlog.inc

--perl
  use strict;
  use warnings;
  use Compress::Zlib qw(crc32);

  my $file = $ENV{'PATCHED_BINLOG_PATH'};
  my $event_code = $ENV{'PATCHED_EVENT_CODE'};
  my $tag = $ENV{'PATCHED_TAG'};
  my $new_length = $ENV{'PATCHED_TAG_LENGTH'};
  my $has_checksums = $ENV{'PATCHED_BINLOG_HAS_CHECKSUMS'};

  my $common_header_length = 19;
  my $footer_length = 4;

  # values < 128 are encoded as a single byte: (value << 1)
  length($tag) < 128 && $new_length < 128 or die "lengths must be < 128";
  my $encoded_tag = pack('C', length($tag) << 1) . $tag;

  open(my $fh, '+<:raw', $file) or die "cannot open '$file': $!";
  my $file_size = -s $fh;
  # skipping the 4-byte binlog magic
  my $event_offset = 4;
  my $patched = 0;
  while (!$patched && $event_offset + $common_header_length <= $file_size) {
    seek($fh, $event_offset, 0) or die "cannot seek in '$file': $!";
    read($fh, my $header, $common_header_length) == $common_header_length
      or die "cannot read the event common header";
    my $event_size = unpack('V', substr($header, 9, 4));
    $event_size >= $common_header_length or die "invalid event size";

    if (ord(substr($header, 4, 1)) == $event_code) {
      seek($fh, $event_offset, 0) or die "cannot seek in '$file': $!";
      read($fh, my $event, $event_size) == $event_size
        or die "cannot read the event";
      my $tag_position = index($event, $encoded_tag, $common_header_length);
      if ($tag_position >= 0) {
        substr($event, $tag_position, 1) = pack('C', $new_length << 1);
        if ($has_checksums) {
          my $payload_size = $event_size - $footer_length;
          substr($event, $payload_size, $footer_length) =
            pack('V', crc32(substr($event, 0, $payload_size)));
        }
        seek($fh, $event_offset, 0) or die "cannot seek in '$file': $!";
        print $fh $event or die "cannot write to '$file': $!";
        $patched = 1;
      }
    }
    $event_offset += $event_size;
  }
  close($fh) or die "cannot close '$file': $!";
  $patched or die "no event of type $event_code with tag '$tag' found in '$file'";
EOF

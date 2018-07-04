#!/usr/bin/env ruby

=begin
$ cd ~/Documents/workspace/hadoop-book
$ hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
-input input/ncdc/all \
-output output \
-mapper ch02/src/main/ruby/max_temperature_map.rb \
-reducer ch02/src/main/ruby/max_temperature_reduce.rb \
-file ch02/src/main/ruby/max_temperature_map.rb \
-file ch02/src/main/ruby/max_temperature_reduce.rb
=end

STDIN.each_line do |line|
  val = line
  year, temp, q = val[15,4], val[87,5], val[92,1]
  puts "#{year}\t#{temp}" if (temp != "+9999" && q =~ /[01459]/)
end
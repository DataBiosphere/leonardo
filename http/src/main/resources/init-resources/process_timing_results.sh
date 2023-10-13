#!/opt/homebrew/bin/bash


#
# full information here: https://docs.google.com/document/d/10eW-HvcmLZ0NJ-vtLU2rzAKznZ9HvOiJT73Jj9zfPJY/edit?usp=sharing
# 
# basic:
#
# azure_vm_init_script.sh has been instrumented with timing info
#
# after a VM has initialized, copy the timing log locally:
#    scp leoAdmin@40.124.170.200:/tmp/timing_results.log timing_results.log
#
# Run this parse script (in the directory with the azure_vm_init_script.sh script) 
#    process_results.sh timing_results.log
#
# results are in init_script_timings.tsv



function parse_timing {
    lineno=0
    timestamp=0
    sum=0

    while [ $timestamp -ge 0 ]; do

        read -r next_lineno next_timestamp 

        result=$((next_timestamp - timestamp))

        if [ $result -le 1000000 ] && [ $result -gt 0 ]
        then
            sum=$((sum + result))
            printf "%6.6s %s\n" "$lineno" "$result"
        fi
        timestamp=$next_timestamp
        lineno=$next_lineno
        if [ -z $lineno ]; then
            break
        fi
        
    done < /dev/stdin

    echo "Total: $sum"
}

if [ $# -eq 0 ] ;   then
   echo "Usage: process_results.sh <input file name>"
   exit 1
fi

INPUT_FILE=$1
echo "Processing: $INPUT_FILE"

# parse the log to obtain time spent on each line
cut -c -24 $INPUT_FILE | grep TIMING: | sed 's/TIMING: //g' | sed 's/://g' | sed 's/^T//' | parse_timing | sort -k1b,1 > times.sorted

# order the init script into the same format as the parsed results
cat -n azure_vm_init_script.sh | sort -k1b,1 > init.sorted

# join the parsed results with the init script
join times.sorted init.sorted | sort -n | awk '{printf("%s\t%s\t", $1, $2)}{ for (i=3; i<=NF; i++) printf("%s ",$i)}{ print"" }' | sort -k2 -n -r > init_script_timings.tsv


rm times.sorted
rm init.sorted

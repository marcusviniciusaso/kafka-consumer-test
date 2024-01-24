#!/bin/bash
is_number() {
  if [[ "$1x" == "x" ]] ; then
    return 1
  fi    
  re='^[0-9]+$'
  if ! [[ $1 =~ $re ]] ; then
    return 1
  fi
  return 0
}

for n in {1..3}
do
  echo "Pass $n:"	
  for i in $(ls /proc/)
  do
    is_number $i
    if [ $? -ne 0 ]; then
      continue;	  
    fi  
    
    RESULT=$(cat /proc/$i/cmdline 2>/dev/null | grep "kafka-checker"| wc -l)

    if [[ "$RESULT" == "0"  ]]; then
      continue;	  
    fi

    echo "Process $i" 
    echo "cmdline: /proc/$i/cmdline"
    echo "Killing process:"
    kill -9 $i
    echo "Done. Checking if there are more processes..."
    sleep 2  
  done
done
echo "End"

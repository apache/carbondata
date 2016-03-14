#!/bin/sh
cd  release

generateChecksum(){
find "$PWD" -name \** | while read file; 
	do  
		if [[ "$file" == *.sha256 ]] || [[ "$file" == *.txt ]]  || [[ "$file" == *.bat ]] || [[ "$file" == *.sh ]]; then
      			continue;
     		fi
		if [ -d "$file" ]; then	
			continue;
    		fi
		cd "${file%/*}"; 
		sha256sum -b "${file##*/}" > "${file##*/}".sha256 ;	
      done

}

generateChecksum


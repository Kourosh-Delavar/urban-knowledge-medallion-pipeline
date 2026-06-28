import os
import sys

def check_hadoop_config():
    
    print("Check for potential Hadoop S3A configuration conflicts...")
    
    risky_keys = [
        "fs.s3a.connection.timeout",
        "fs.s3a.connection.establish.timeout",
        "fs.s3a.threads.keepalivetime"
    ]
    
    # if these are set in system env variables or files, 
    # check if they contain characters that are not digits.
    for key in risky_keys:
        val = os.environ.get(key)
        if val:
            print(f"Check environment variable {key}: '{val}'")
            if not val.isdigit():
                print(f"[FATAL] Detected non-numeric value '{val}' for {key}.")
                print("Hadoop will crash with NumberFormatException.")
    
    print("Check complete. If you saw a FATAL/CRITICAL warning, override it in SparkSession builder.")

if __name__ == "__main__":
    check_hadoop_config()

# env2conf syntax:
# _ -> .
# __XYZ -> Xyz
# ___ -> -
# ^QWE_RTY -> rty
# Samples:
# CFG_LIVY_SERVER_ENABLE_HIVE___CONTEXT -> livy.server.enable.hive-context
# CFG_SPARK_HADOOP_FS_DEFAULT__F__S -> spark.hadoop.fs.defaultFS

import sys
for l in sys.stdin:
    k, v = l[:-1].split('=', 1)
    k = k.replace("___", "-")
    k = ''.join(w[0].upper() + w[1:].lower() for w in k.split("__"))
    k = k.replace("_", ".")
    k = ".".join(k.split(".")[1:])
    print(k + ' ' + v)

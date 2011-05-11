# Set CIS-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/usr/java/default
export NWITTER_HOME=/mnt/nwitter/manager
export NWITTER_OPTS="-server -Xms1024M -Xmx1024M -Dipc.client.connection.maxidletime=3600000"
export NWITTER_LOG_DIR=${NWITTER_HOME}/logs

export NWITTER_MANAGER_OPTS="-Xms1024M -Xmx4096M"

for f in $NWITTER_HOME/lib/*.jar; do
if [ "$JARLIBS" = "" ] ; then
	JARLIBS=file://$f;
else
    JARLIBS=${JARLIBS},file://$f;
fi
done
export NWITTER_WORKFLOW_OPTS=-Dhadoop.root.logger=INFO,console 

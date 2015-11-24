# define base image
FROM zsibio/docker-base

# copy script, default configuration and uber jar
ADD scripts/zsi-bio-cnv.sh              ./
ADD scripts/application.conf            ./
ADD target/scala-2.10/zsi-bio-cnv.jar   ./

# set default action
CMD ["./zsi-bio-cnv.sh"]
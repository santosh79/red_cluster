Gem::Specification.new do |s|
  s.name     = 'red_cluster'
  s.version  = "0.0.2"
  s.date     = Time.now.strftime('%Y-%m-%d')
  s.summary  = "Red Cluster clusters togethers a set of redis servers."
  s.homepage = "https://github.com/santosh79/red_cluster"
  s.email    = "santosh79@gmail.com"
  s.authors  = ["Santosh Kumar"]

  s.files    = Dir.glob("lib/**/*")
  s.files   += Dir.glob("spec/**/*")
  s.add_dependency "redis"

  s.description = <<description
    Red Cluster brings together a set of redis servers and allows you to read and write to them
    as though you were writing to just one. A few of the reasons you might want to consider
    clustering could be:

    * Robustness - Having a write master and read slaves
    * Harnessing the multiple cores you have running while not compromising on the speed of redis
    * Fault tolerance - When one of the masters goes down a slave in the replica sets gets promoted
                        automatically, with no down-time
description
end

# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/xenial64"

  config.vm.define "rqlite" do |rqlite|
    config.vm.network "private_network", ip: "192.168.200.10"
    config.vm.provision :shell, :path => "vagrant_setup.sh"
    config.vm.provision :shell, :inline => "rqlited ~/node.1 &"

    if not ENV['CLUSTER_SIZE'].nil?
      port = 4001
      (2..ENV['CLUSTER_SIZE'].to_i).each do |i|
        port = port + 1
        config.vm.provision :shell, :inline => "rqlited -join localhost:4001 -raft-addr localhost:#{port} ~/node.#{i} &"
      end
    end

  end

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "2048"]
  end

end

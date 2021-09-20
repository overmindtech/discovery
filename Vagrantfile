# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  # Manage the virtualbox extensions using the vagrant-vbguest plugin
  config.vbguest.auto_update = true

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  # config.vm.synced_folder "../data", "/vagrant_data"
  config.vm.synced_folder ".", "/vagrant", type: "virtualbox"

  # Get the Go version from the current directory and mirror this
  go_version = `go version`.match(/go version\s(\S*)/)[1]

  config.vm.provider "virtualbox" do |v|
      v.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
      v.memory = 1024
      v.cpus = 2
  end

  config.vm.define "centos" do |centos|
    centos.vm.box = "generic/centos8"
    centos.vm.network "private_network", ip: "192.168.158.10"

    centos.vm.provision "shell", inline: <<-SHELL
      # Patch
      yum update -y

      # Add Puppet repos
      sudo rpm -Uvh https://yum.puppet.com/puppet6-release-el-8.noarch.rpm

      # Install git dependencies
      yum install git puppet-agent epel-release net-tools -y
    SHELL
  end

  config.vm.define "ubuntu" do |ubuntu|
    ubuntu.vm.box = "generic/ubuntu2004"
    ubuntu.vm.network "private_network", ip: "192.168.158.11"

    ubuntu.vm.provision "shell", inline: <<-SHELL
      # Add Puppet repos
      wget https://apt.puppetlabs.com/puppet6-release-bionic.deb
      dpkg -i puppet6-release-bionic.deb
      apt update

      # Patch
      apt-get upgrade -y

      # Install git dependencies
      apt-get install git puppet-agent -y
    SHELL
  end

  config.vm.provision "shell", inline: <<-SHELL
    # Install git
    if [ -n "$(command -v yum)" ]
    then
      yum install git -y
    elif [ -n "$(command -v yum)" ]
    then
      apt install git -y
    fi

    # Turn off selinux. Would be better to have this on but for now it's off
    sed -i 's/^SELINUX=.*/SELINUX=disabled/g' /etc/selinux/config
    setenforce 0

    # Download Go
    curl -O https://dl.google.com/go/#{go_version}.linux-amd64.tar.gz

    # Extract
    tar -C /usr/local -xzf go*.tar.gz

    # Add to the path
    echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> $HOME/.bash_profile

    # Add to local path
    source $HOME/.bash_profile

    # Install the debugger
    go get -u github.com/go-delve/delve/cmd/dlv

    # Set up symlinks
    mkdir -p /root/go/src/github.com/dylanratcliffe
    ln -s /vagrant /root/go/src/github.com/dylanratcliffe/deviant_cli

    # Install dependencies
    cd /root/go/src/github.com/dylanratcliffe/deviant_cli
    go get -v -t -d ./...
    cd -

    # Add hosts entry for NATS debug server
    echo "10.0.2.2 nats.debug" >> /etc/hosts
  SHELL
end

def externalIp(config, host):
  return host + config.get('Basic', 'external.ip')
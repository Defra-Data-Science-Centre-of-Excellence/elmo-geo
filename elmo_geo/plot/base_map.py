import contextily as ctx

os_provider = ctx.providers.OrdnanceSurvey.Light(key=dotenv.get_key('.env', 'OS_KEY'))

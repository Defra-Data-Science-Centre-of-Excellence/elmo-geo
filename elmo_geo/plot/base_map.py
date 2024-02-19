import contextily as ctx
import dotenv

os_provider = ctx.providers.OrdnanceSurvey.Light(key=dotenv.get_key('.env', 'OS_KEY'))

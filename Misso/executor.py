from Misso.manager.server import Server

config = "config/strategy_manager.yaml"

server = Server(config)
server.run(loop=True)
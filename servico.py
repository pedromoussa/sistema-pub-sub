from __future__ import annotations
import signal
import rpyc
import threading
from rpyc.utils.server import ThreadedServer
from typing import Callable, TYPE_CHECKING
from dataclasses import dataclass
# Se não funcionar no lab rode:
# $ pip install --user typing_extensions
import sys
IS_NEW_PYTHON: bool = sys.version_info >= (3, 8)
if IS_NEW_PYTHON:
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

UserId: TypeAlias = str
Topic: TypeAlias = str

# Isso é para ser tipo uma struct
# Frozen diz que os campos são read-only
if IS_NEW_PYTHON:
    @dataclass(frozen=True, kw_only=True, slots=True)
    class Content:
        author: UserId
        topic: Topic
        data: str
elif not TYPE_CHECKING:
    @dataclass(frozen=True)
    class Content:
        author: UserId
        topic: Topic
        data: str

if IS_NEW_PYTHON:
    FnNotify: TypeAlias = Callable[[list[Content]], None]
elif not TYPE_CHECKING:
    FnNotify: TypeAlias = Callable

class BrokerGlobals:
    topics = {}
    subscribers = {}
    admin_id = "admin"
    connections = {}
    user_callbacks = {}

class BrokerService(rpyc.Service):
    running = True

    def create_topic(self, user_id: UserId, topic_name: str) -> Topic:
        if user_id != BrokerGlobals.admin_id:
            return "Apenas o Administrador pode criar tópicos."
        if topic_name in BrokerGlobals.topics:
            return "Tópico já existente."
        BrokerGlobals.topics[topic_name] = []
        return f"Tópico '{topic_name}' criado com sucesso."

    def exposed_login(self, user_id: UserId, callback: FnNotify) -> bool:
        if user_id in BrokerGlobals.subscribers:
            if BrokerGlobals.connections.get(user_id):
                return False
            BrokerGlobals.connections[user_id] = self._conn
        else:
            BrokerGlobals.subscribers[user_id] = []
            BrokerGlobals.connections[user_id] = self._conn
        # Atualiza callback do usuario
        BrokerGlobals.user_callbacks[user_id] = callback

        for topic in BrokerGlobals.subscribers[user_id]:
            self._send_previous_ads(user_id, topic)

        return True

    def exposed_list_topics(self) -> List[Topic]:
        return list(BrokerGlobals.topics.keys())

    def exposed_publish(self, user_id: UserId, topic: Topic, data: str) -> bool:
        if topic not in BrokerGlobals.topics:
            return False
        content = Content(author=user_id, topic=topic, data=data)
        serialized_content = content
        BrokerGlobals.topics[topic].append(serialized_content)
        self._notify_subscribers(topic, [serialized_content])
        return True

    def exposed_subscribe_to(self, user_id: UserId, topic: Topic) -> bool:
        if topic not in BrokerGlobals.topics:
            return False
        BrokerGlobals.subscribers[user_id].append(topic)
        self._send_previous_ads(user_id, topic)
        return True

    def exposed_unsubscribe_to(self, user_id: UserId, topic: Topic) -> bool:
        if topic not in BrokerGlobals.topics:
            return False
        BrokerGlobals.subscribers[user_id] = [t for t in BrokerGlobals.subscribers[user_id] if t != topic]
        return True

    def on_connect(self, conn):
        self._conn = conn

    def on_disconnect(self, conn):
        user_id = next((user_id for user_id, c in BrokerGlobals.connections.items() if c == conn), None)
        if user_id:
            del BrokerGlobals.connections[user_id]
            del BrokerGlobals.user_callbacks[user_id]

    def _notify_subscribers(self, topic: Topic, contents: List[Content]) -> None:
        for user_id, subscriptions in BrokerGlobals.subscribers.items():
            conn = BrokerGlobals.connections.get(user_id)
            if conn:
                for sub in subscriptions:
                    if sub == topic:
                        callback = BrokerGlobals.user_callbacks[user_id]
                        # Cria nova thread para cada chamada do callback
                        threading.Thread(target=self._invoke_callback, args=(callback, contents)).start()

    @staticmethod
    def _invoke_callback(callback: FnNotify, contents: List[Content]) -> None:
        try:
            callback(contents)
        except Exception as e:
            print(f"Ocorreu um erro durante a execução do callback: {e}")

    def _send_previous_ads(self, user_id: UserId, topic: Topic) -> None:
        if topic in BrokerGlobals.topics:
            previous_ads = []
            for content in BrokerGlobals.topics[topic]:
                previous_ads.append(content)
            if previous_ads:
                callback = BrokerGlobals.user_callbacks[user_id]
                threading.Thread(target=self._invoke_callback, args=(callback, previous_ads)).start()

    @staticmethod
    def start_console_input(exit_event: threading.Event):
        broker_service = BrokerService()
        while not exit_event.is_set():
            user_input = input("Digite 'create_topic' seguido pelo tópico que deseja criar ou 'exit' para sair: ")
            if user_input.strip() == "exit":
                exit_event.set()
                break
            broker_service.handle_console_input(user_input)

    def handle_console_input(self, user_input):
        command, *args = user_input.split()
        if command == "create_topic":
            self.create_topic(BrokerGlobals.admin_id, *args)

    @staticmethod
    def stop_server(signal, frame):
        print("Finalizando Broker...")
        rpyc.ThreadedServer.stop_all()

if __name__ == '__main__':
    exit_event = threading.Event()
    server = ThreadedServer(BrokerService, port=10001, protocol_config={'allow_public_attrs':True})
    signal.signal(signal.SIGINT, BrokerService.stop_server)
    server_thread = threading.Thread(target=server.start)
    # Inicia thread servidor
    server_thread.start()

    # Inicia loop de input do terminal
    threading.Thread(target=BrokerService.start_console_input, args=(exit_event,)).start()

    # Aguarda o evento de saída ser acionado
    exit_event.wait()

    # Encerra o servidor e aguarda a thread do servidor terminar
    server.close()
    server_thread.join()

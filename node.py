import socket
import threading
import sys
import time
import random

# Dichiariamo i 5 nodi a quali porte rispondono
NODES = {
    1: 5001,
    2: 5002,
    3: 5003,
    4: 5004,
    5: 5005
}

class BullyNode:
    def __init__(self, node_id):
        self.id = node_id
        self.port = NODES[node_id]
        self.coordinator = None
        
        # di default impostiamo lo stato di ogni nodo per l'elezione 
        self.election_in_progress = False
        self.received_answer = False
        self.running = True
        
        # T: tempo massimo di attesa per risposte ANSWER (in secondi)
        self.T = 4.0
        # T': tempo massimo di attesa per il messaggio COORDINATOR (in secondi)
        self.T_prime = 6.0

        # Inizializzazione Socket 
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(('localhost', self.port))
        self.server.listen(5)

    def log(self, msg):
        # stampiamo il log del nodo
        print(f"[P{self.id}] {msg}")

    def send_msg(self, dest_id, msg):
        #Invia uno dei messaggi obbligatori su socket TCP.
        #Ritorna True se inviato, False in caso di connessione rifiutata (nodo guasto).
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect(('localhost', NODES[dest_id]))
            s.sendall(msg.encode('utf-8'))
            s.close()
            return True
        except (ConnectionRefusedError, socket.timeout):
            return False

    def handle_client(self, conn):
        # messaggi in ingresso
        try:
            data = conn.recv(1024).decode('utf-8')
            if not data: return
            parts = data.split()
            cmd = parts[0]
            sender_id = int(parts[1])

            # 1. messaggio di Election
            if cmd == "ELECTION":
                # Il nodo risponde con ANSWER e avvia la propria elezione se non in corso
                self.send_msg(sender_id, f"ANSWER {self.id}")
                if not self.election_in_progress:
                    threading.Thread(target=self.start_election).start()

            # 2. messaggio di ANSWER
            elif cmd == "ANSWER":
                self.received_answer = True

            # 3.  messaggio COORDINATOR
            elif cmd == "COORDINATOR":
                self.coordinator = sender_id
                # Arresto delle elezioni in corso alla ricezione di COORDINATOR
                self.election_in_progress = False
                self.log(f"Riconosco P{sender_id} come COORDINATORE")

        except Exception:
            pass
        finally:
            conn.close()

    def listen_server(self):
        #loop di ascolto dei messaggi
        while self.running:
            try:
                self.server.settimeout(1.0)
                conn, addr = self.server.accept()
                threading.Thread(target=self.handle_client, args=(conn,)).start()
            except socket.timeout:
                continue
            except Exception:
                if self.running:
                    break

    def start_election(self):
        #algoritmo del bullo, inizio
        self.election_in_progress = True
        self.received_answer = False
        self.log("Avvio elezione")

        # Invia ELECTION a tutti i nodi con ID più alto
        higher_nodes = [n for n in NODES if n > self.id]
        if not higher_nodes:
            self.log("Nessun nodo con ID superiore")
            self.become_coordinator()
            return

        for n in higher_nodes:
            self.send_msg(n, f"ELECTION {self.id}")

        # Timeout T: Attesa di risposte ANSWER
        time.sleep(self.T)
        
        if not self.received_answer:
            highest_nodes_str = ", ".join([f"P{n}" for n in higher_nodes])
            self.log(f"Nessuna risposta da {highest_nodes_str}")
            self.become_coordinator()
        else:
            # Timeout T': Abbiamo ricevuto ANSWER, attendiamo il COORDINATOR
            time.sleep(self.T_prime)
            if self.election_in_progress:
                self.log("Timeout: coordinatore non ha inviato COORDINATOR. Riavvio elezione.")
                self.start_election()

    def become_coordinator(self):
        #procalmazione coordinator
        self.coordinator = self.id
        self.election_in_progress = False
        self.log("Mi dichiaro COORDINATORE")
        
        # comunico coordinato a tutti i nodi inferiori
        lower_nodes = [n for n in NODES if n < self.id]
        for n in lower_nodes:
            self.send_msg(n, f"COORDINATOR {self.id}")

    def check_coordinator(self):
        #simulazione guato con tantativo di connessione con il coordinator
        if not self.coordinator or self.coordinator == self.id:
            return
            
        try:
            # Tentativo di aprire un socket TCP con il coordinatore
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect(('localhost', NODES[self.coordinator]))
            s.close()
        except (ConnectionRefusedError, socket.timeout):
            # Rilevazione del guasto tramite timeout.
            if not self.election_in_progress:
                self.log("Timeout: coordinatore non risponde")
                self.coordinator = None
                self.start_election()

    def heartbeat(self):
        #controllo se coordinator ancora attivo
        while self.running:
            time.sleep(random.uniform(3.0, 5.0))
            if self.coordinator and not self.election_in_progress:
                self.check_coordinator()

    def run(self):
        #avvio ascolto, heartbet e gestione input terminale
        threading.Thread(target=self.listen_server, daemon=True).start()
        threading.Thread(target=self.heartbeat, daemon=True).start()
        
        try:
            while self.running:
                cmd = input().strip()
                if cmd == "start":
                    threading.Thread(target=self.start_election).start()
        except KeyboardInterrupt:
            # cattura Ctrl+C per simulare il guasto 
            self.log("*** processo terminato ***")
            self.running = False
            self.server.close()
            sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 2 or int(sys.argv[1]) not in NODES:
        print("Uso: py node.py <id_nodo (1-5)>")
        sys.exit(1)
        
    node = BullyNode(int(sys.argv[1]))
    node.run()

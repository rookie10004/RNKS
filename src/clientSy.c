#include <stdio.h> 
#include <string.h> 
#include <errno.h> 
#include <stdlib.h> 
#include <unistd.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/select.h>
#include <fcntl.h> //! filecontrol
#include <time.h>

#include "data.h"
#include "config.h"
#include "clientSy.h"

//! zeit hilffunktion
double getCurrentTime(void)
{
    //! unabhängig von system-uhr
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    //! sekunden + nanosekunden (genauigkeit)
    return ts.tv_sec + (ts.tv_nsec / 1e9);
}

/*
 * clientSy.c
 *
 * Diese Datei soll die ARQ-/Go-Back-N-Sende- und das UDP Handling
 * für den Client enthalten.
 *
 * WICHTIG:
 *   - Dateiname und Funktionssignaturen in clientSy.h sollen
 *     unverändert beibehalten werden.
 *
 * Aufgaben:
 *   - UDP-Socket (IPv6, Datagram) erzeugen, Serveradresse auflösen
 *   - ARQ-/GBN-Sendealgorithmus implementieren:
 *        * Fensterverwaltung (base, nextNum, packetCount)
 *        * innerhal eines Intervalls max. 1 Paket senden und empfangen (GBN_TIMEOUT_INT_MS)
 *        * Paket-Timeout in Intervallen (GBN_TIMEOUT_UNITS)
 *        * Retransmission der unbestätigten Pakete
 *   - kumulative ACKs (AnswOk.SeNo) auswerten
 *   - Hello/Data/Close über die gemeinsame Logik abwickeln
 */

/* Optionale globale Variablen:
 *   - Socket-Deskriptor
 *   - Serveradresse
 *   - GBN-Sendezustand (Fensterbasis, nächster freier Platz etc.)
 */

int socketFd = -1; //! socked file descriptor
struct sockaddr_storage serverAddr; //! server adresse
socklen_t serverAddrLen = 0; //! Länge IPv6 Adresse (die wir wirklich nutzen)

/* --------------------------------------------------------------- */
/*  Initialisierung / Abschluss                                   */
/* --------------------------------------------------------------- */

void initClient(char *name, const char *port)
{
    /* TODO:
     *  - mit getaddrinfo(...) die Serveradresse ermitteln
     *  - UDP/IPv6-Socket erzeugen
     *  - Socket non-blocking setzen (fcntl)
     *  - Adresse und Socket in globalen Variablen speichern
    */

    //! adress suche
    struct addrinfo hints;      //! anfrage formular
    struct addrinfo *result;    //! suchergebnis (liste) 

    memset(&hints, 0, sizeof(struct addrinfo)); //! hints speicher auf 0
    hints.ai_family = AF_INET6;    //! nur ipv6 adressen suchen
    hints.ai_socktype = SOCK_DGRAM; //! UDP (Datagramme) nutzen
    
    int status;                 //! fehlerprüfung
    status = getaddrinfo(name, port, &hints, &result); //! suche für name, port, hints, gibt liste von addrinfos zurück
    if (status != 0) 
    {
        fprintf(stderr, "client getaddrinfo error");
        exit(EXIT_FAILURE);
    }

    struct addrinfo* current;
    for (current = result; current != NULL; current = current->ai_next) //! ergebnisse durchgehen (linked list, server kann mehrere adressen haben (ipv4 und ipv6))
    {
        //! socket erstellen von erstem funktionierenden eintrag
        socketFd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        
        if (socketFd == -1) //! nächster eintrag, bis erfolgreich
        {
            continue; 
        }

        //! socket timeout abrufen
        int flags = fcntl(socketFd, F_GETFL, 0);

        //! flag O_NONBLOCK hinzufügen
        if (fcntl(socketFd, F_SETFL, flags | O_NONBLOCK) < 0) 
        {
            perror("client error fcntl");
            exit(EXIT_FAILURE);
        }

        printf("client init für %s, %s\n", name, port);
        
        //! gefundene adresse in global variable kopieren
        memcpy(&serverAddr, current->ai_addr, current->ai_addrlen);
        serverAddrLen = current->ai_addrlen; 

        break; 
    }

    //! liste freigeben 
    freeaddrinfo(result); 

    if (current == NULL) //! testen ob überhaupt ein socket erstellt wurde oder (Ende der Liste erreicht)
    {
        fprintf(stderr, "client kein socket für %s, %s\n", name, port);
        exit(EXIT_FAILURE);
    }
    
    printf("client init für %s, %s\n", name, port);
}

void closeClient(void)
{
    /* TODO:
     *  - ggf. offenen Socket schließen
     *  - globale Zustandsvariablen zurücksetzen
     */

    if (socketFd >= 0) //! wenn offenes socket, dann schließen
    {
        close(socketFd); 
        socketFd = -1;   
    }
}

/* --------------------------------------------------------------- */
/*  Interne Sende-Logik (GBN/ARQ)                                  */
/* --------------------------------------------------------------- */

/*
 * doRequest:
 *   - ReqHello: einmaliger Handshake mit eigenem Timeout
 *   - ReqData / ReqClose: Go-Back-N-Sendealgorithmus mit Fenster 
 *
 * Parameter:
 *   req        : zu sendendes Request-Paket
 *   winSize    : Fenstergröße (1..GBN_MAX_WINDOW)
 *   windowFull : optionaler Rückgabewert, ob das Sendefenster voll ist
 *
 * Rückgabewert:
 *   - Zeiger auf empfangene Antwort (struct answer)
 *   - NULL, wenn in diesem Intervall keine relevante Antwort
 *     eingetroffen ist
 *
 * TODO:
 *   - Für ReqHello:
 *        * Fensterzustand zurücksetzen
 *        * Hello-Paket einmal senden
 *        * mit select() und separatem Timeout auf Antwort warten
 *   - Für Data/Close:
 *        * Sende­fenster im Ringpuffer verwalten (base, nextNum, packetCount)
 *        * pro Intervall höchstens EIN Paket senden (neu oder Retransmit)
 *        * Timeout in Einheiten von Intervallen zählen
 *        * kumulative ACKs auswerten und Fensterbasis verschieben
 */
static struct answer *doRequest(struct request *req, int winSize, int *windowFull, int *retransmission)
{
    (void)req;
    (void)winSize;
    (void)windowFull;
	(void)retransmission;

    /* TODO:
     *  - GBN-/ARQ-Sendealgorithmus implementieren
     *  - select() zur Realisierung der Sendeintervalle verwenden
     *  - bei Timeout Retransmissions auslösen
     *  - Antwortpaket (struct answer) bei Erfolg zurückgeben
     */
    return NULL;
}

/* --------------------------------------------------------------- */
/*  Externe ARQ-API (Wrapper um doRequest)                         */
/* --------------------------------------------------------------- */

int arqSendHello(int winSize)
{
    /* TODO:
     * - struct request für Hello vorbereiten (ReqType = ReqHello)
     * - Sequenznummern- und Fensterzustand initialisieren
     * - doRequest(...) aufrufen
     * - Antwort auswerten (AnswHello / AnswOk)
     * - bei Fehler einen Wert != 0 zurückgeben
     */

    struct request req;     //! paket zu senden
    ssize_t numBytes;       //! anzahl gesendet/empfangen

    //! variablen für timer und versuche
    double startTime;       
    double currentTime;     
    int versuche = 0;       
    const int maxVersuche = 5; //! error nach 5 versuchen
    const double timeout = 3.0; //! wartezeit

    //! request ausfüllen
    memset(&req, 0, sizeof(req));   //! speicher auf 0
    req.ReqType = ReqHello;         
    req.SeNr = 0;                   
    req.FlNr = 0;                   

    printf("client sendet HALLO\n");

    //! äußere schleife, wiederholt senden
    do 
    {
        versuche++;

        //! request über socket an server senden
        numBytes = sendto(socketFd, 
                          &req,
                          sizeof(req), 
                          0, 
                          (struct sockaddr*)&serverAddr, //! addresse generisch casten
                          serverAddrLen);

        if (numBytes < 0) //! testen wieviele bytes gesendet
        {
            perror("client sendto error");
            return -1;
        }

        //! sendezeit merken
        startTime = getCurrentTime();

        struct sockaddr_storage senderAddr; //! addresse wer geantwortet hat
        socklen_t addrLen;    
        struct answer ack;      //! antwort

        //! innere schleife, auf antwort warten
        while(1)
        {
            //! auf antwort warten 
            addrLen = sizeof(senderAddr);
            memset(&ack, 0, sizeof(ack)); 
            
            numBytes = recvfrom(socketFd, 
                                &ack, //! quittung
                                sizeof(ack), 
                                0, 
                                (struct sockaddr*)&senderAddr,
                                &addrLen);

            //! antwort erhalten
            if (numBytes > 0)
            {
                //! prüfen ob antwort AnswHello / AnswOk
                if (ack.AnswType == AnswHello || ack.AnswType == AnswOk) 
                {
                    printf("client HELLO verbindung\n");
                    return 0; 
                } else 
                {
                    fprintf(stderr, "client unknown answer: %c\n", ack.AnswType);
                }
            }

            //! keine antwort erhalten
            if (numBytes < 0) //! testen wieviele bytes empfangen
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK) //! error auswerten 
                {
                    //timeout prüfen
                    currentTime = getCurrentTime();

                    if ((currentTime - startTime) > timeout) 
                    {
                        printf("client timeout hello, versuch %d...\n", versuche);
                        break; //! aus schleife austreten / neu senden
                    }
                    
                    continue; //! timeout nochnicht erroicht
                } 
                else 
                {
                    perror("client recvfrom error"); //! anderer error 
                    return -1;
                }
            }
        } 

    } while (versuche < maxVersuche); 

    fprintf(stderr, "client hello gescheitert (timeout)\n");
    return -1;
}

int arqSendData(const struct app_unit *app, int winSize)
{
    /* TODO:
     *  - aus app eine struct request mit ReqType = ReqData erzeugen:
     *       * FlNr = app->len (ggf. begrenzen auf PufferSize)
     *       * SeNr = laufende Paketnummer (z.B. statischer Zähler)
     *       * Nutzdaten kopieren
     *  - doRequest(...) aufrufen
     *  - Antwort auswerten (AnswOk/AnswWarn/AnswErr)
	 *  - bei bufferFull oder retransmission doRequest mit unverändertem req erneut aufrufen
     *  - bei Fehler einen Wert != 0 zurückgeben
     */
    
    static unsigned long currentSeqNum = 1; //! zähler für pakete

    struct request req; //! paket vom client
    
    //! header ausfüllen
    memset(&req, 0, sizeof(req)); //! speicher auf 0
    req.ReqType = ReqData; //! typ ist DATA
    req.SeNr = currentSeqNum; //! aktuelle paket nummer
    req.FlNr = app->len; //! bytes an nutzdaten
    
    memcpy(req.name, app->data, app->len); //! daten in "name" kopieren
    
    printf("client paket nr. %lu (%lu bytes)\n", req.SeNr, req.FlNr);

    //! alles für timer & versuch counter
    double startTime;
    double currentTime;
    int versuche = 0;
    const int maxVersuche = 5;
    const double timeout = 3.0;

    //! äußere schleife für timeout wiederholungen
    do 
    {
        versuche++;

        //! daten senden
        ssize_t numBytes;
        numBytes = sendto(socketFd, 
            &req,
            sizeof(req), 
            0, 
            (struct sockaddr*)&serverAddr,
            serverAddrLen);
        
        if (numBytes < 0) 
        {
            perror("cleint error sendto");
            return -1;
        }

        startTime = getCurrentTime();

        //! innere schleife zum warten auf antwort
        while(1)
        {
            //! auf bestätigung warten 
            struct answer ack; //! paket vom server
            socklen_t addrLen;
            struct sockaddr_storage senderAddr;

            addrLen = sizeof(senderAddr);
            memset(&ack, 0, sizeof(ack)); //! speicher auf 0

            numBytes = recvfrom(socketFd, 
                                &ack, sizeof(ack), 
                                0, 
                                (struct sockaddr*)&senderAddr,
                                &addrLen);

            //! antwort / ack erhalten
            if (numBytes > 0) 
            {
                if (ack.AnswType == AnswOk || ack.AnswType == AnswHello) 
                {
                    printf("client paket %lu ack\n", currentSeqNum);
                    currentSeqNum++;
                    return 0; 
                }
            }

            //! keine antwort
            if (numBytes < 0) 
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK) //! error testen (automatisch vom operationssystem gesetzt)
                {
                    currentTime = getCurrentTime(); //! zeit abfragen
                    if ((currentTime - startTime) > timeout) //! aus schleife austreten, wenn timout abgelaufen
                    {
                        printf("client timeout für %lu sende versuch %d...\n", currentSeqNum, versuche);
                        break; 
                    }
                    
                    //! bleibt in schleife, wenn kein timeout erriecht 
                    continue;

                } else 
                {
                    perror("client recvfrom error"); //! anderer error
                    return -1;
                }
            }
        }
        
    } 
    while (versuche < maxVersuche);

    fprintf(stderr, "client gibt auf, server nicht erriechbar");
    return -1;
}

int arqSendClose(int winSize) 
{
    /* TODO:
     * - struct request mit ReqClose vorbereiten
     * - doRequest(...) aufrufen
     * - Antwort auswerten
     * - bei Fehler einen Wert != 0 zurückgeben
     */
  
    struct request req;
    struct answer ack;
    ssize_t numBytes;
    socklen_t addrLen;
    struct sockaddr_storage senderAddr;

    //! timer und versuche 
    double startTime;
    double currentTime;
    int versuche = 0;
    const int maxVersuche = 3; 
    const double timeout = 3.0; 

    memset(&req, 0, sizeof(req));
    req.ReqType = ReqClose;
    req.SeNr = 0;
    req.FlNr = 0;

    printf("client sende close\n");

    //! äußere schleife / close senden
    do 
    {
        versuche++;

        numBytes = sendto(socketFd, 
                          &req, sizeof(req), 
                          0, 
                          (struct sockaddr *)&serverAddr, serverAddrLen);

        if (numBytes < 0) 
        {
            perror("client error sendto (close)");
            return -1;
        }

        //! timer starten
        startTime = getCurrentTime();

        //! innere schleife wartet auf antwort
        while(1)
        {
            addrLen = sizeof(senderAddr);
            numBytes = recvfrom(socketFd, 
                                &ack, sizeof(ack), 
                                0, 
                                (struct sockaddr *)&senderAddr, &addrLen);

            //! antwort erhalten
            if (numBytes > 0)
            {
                if (ack.AnswType == AnswOk) 
                {
                    printf("client close bestätigt\n");
                    return 0;
                } 
                else {
                    fprintf(stderr, "client error close abgelehnt\n");
                    return 1;
                }
            }

            //! keine antwort
            if (numBytes < 0) 
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK) //! error auswerten
                {
                    currentTime = getCurrentTime(); //! zeit testen
                    if ((currentTime - startTime) > timeout) //! testen ob timeout erreicht
                    {
                        break; //! aus loop ausbrechen / close neu senden
                    }

                    continue; //! weiter warten
                } 
                else 
                {
                    perror("client error revcfrom (ack-close)");
                    return -1;
                }
            }
        } 

    } while (versuche < maxVersuche); 

    fprintf(stderr, "client close timeout\n");
    return -1;
}

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

static int socketFd = -1; //! socked file descriptor
static struct sockaddr_storage serverAddr; //! server adresse
static socklen_t serverAddrLen = 0; //! Länge IPv6 Adresse (die wir wirklich nutzen)

//! RINGPUFFER: 
//! Index=(Sequenznummer−1)%MAX_SIZE
#define MAX_WINDOW_SIZE 10 //! sliding window buffer size
static struct request windowBuffer[MAX_WINDOW_SIZE]; //! speicher für gesendete pakete
static int packetCount = 0; //! zählen wieviele pakete unterwegs

static unsigned long baseNum = 1; //! altestes unbestätigstes Paket (linker Fensterrand)
static unsigned long nextNum = 1; //! nächstes neues Paket (rechter Fensterrand)

static const long timeSlot = 100000; //! 100ms slot
static const double timeout = 0.3; //! 3s timeout

/* ---------------------------------------------------------------  */
/*  Initialisierung / Abschluss                                     */
/* ---------------------------------------------------------------  */

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
        perror("client: ERROR getaddrinfo error\n");
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
            perror("client: ERROR fcntl\n");
            exit(EXIT_FAILURE);
        }
        
        //! gefundene adresse in global variable kopieren
        memcpy(&serverAddr, current->ai_addr, current->ai_addrlen);
        serverAddrLen = current->ai_addrlen; 

        break; 
    }

    //! liste freigeben 
    freeaddrinfo(result); 

    if (current == NULL) //! testen ob überhaupt ein socket erstellt wurde oder (Ende der Liste erreicht)
    {
        fprintf(stderr, "client: ERROR kein socket für %s, %s\n", name, port);
        exit(EXIT_FAILURE);
    }
    
    printf("client: init für %s, %s\n", name, port);
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

    socketFd = -1;
    serverAddrLen = 0;
    packetCount = 0;
    baseNum = 1;
    nextNum = 1;

    printf("client: closed und reset\n");
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
    ssize_t numBytes;
    socklen_t addrLen;
    struct sockaddr_storage senderAddr;
    static struct answer ack; 

    double startTime;
    double currentTime;

    //! hello / close logik
    if (req->ReqType == ReqHello || req->ReqType == ReqClose)
    {
        int versuche = 0;
        const int maxVersuche = 5;

        //! globale zähler zurücksetzen
        if (req->ReqType == ReqHello) 
        {
            baseNum = 1;
            nextNum = 1;
            packetCount = 0;
        }

        //! äußere schleife, wiederholt senden
        do 
        {
            versuche++;

            numBytes = sendto(socketFd, req, sizeof(struct request), 0, 
                              (struct sockaddr*)&serverAddr, serverAddrLen);

            if (numBytes < 0) 
            {
                perror("client: ERROR sendto\n");
                return NULL;
            }

            //! sendezeit merken
            startTime = getCurrentTime();

            //! innere schleife: warten auf ACK oder ablauf des time slots
            while(1)
            {
                fd_set socketDataFds; //! sockets die überwacht werden sollen (für select)
                
                FD_ZERO(&socketDataFds); //! liste leeren
                FD_SET(socketFd, &socketDataFds); //! offenes socket hinzufügen
                
                struct timeval tv; //! zeit
                tv.tv_sec = 0; 
                tv.tv_usec = timeSlot; //!100ms timeout setzen

                //! schlafen bis daten oder timout (100ms)
                //! socket + 1, weil er alle sockets davor prüft 
                //! lässt dockets mit daten in readfds stehen, wird später geprüft
                //! setzt tv zurück
                int retval = select(socketFd + 1, &socketDataFds, NULL, NULL, &tv);

                if (retval == -1) 
                {
                    perror("client: ERROR select\n");
                    return NULL; 
                }
                else if (retval == 0) //! timeout (100ms)
                {
                    //! prüfen ob "großes" timeout 
                    currentTime = getCurrentTime();
                    if ((currentTime - startTime) > timeout) 
                    {
                        perror("client: TIMEOUT hello -> neu senden\n");
                        break; 
                    }
                    continue; 
                }
                else //! daten bereit
                {
                    if (FD_ISSET(socketFd, &socketDataFds)) 
                    {
                        addrLen = sizeof(senderAddr);
                        memset(&ack, 0, sizeof(ack));

                        numBytes = recvfrom(socketFd, &ack, sizeof(ack), 0, 
                                            (struct sockaddr*)&senderAddr, &addrLen);

                        //! antwort testen
                        if (numBytes > 0 && (ack.AnswType == AnswOk || ack.AnswType == AnswHello)) 
                        {
                            printf("client: ack empfangen, typ %c, SeNo %lu\n", ack.AnswType, ack.SeNo);
                            return &ack; 
                        }
                    }
                }
            }
        } while (versuche < maxVersuche); 
        
        perror("client: ERROR alle versuche aufgebraucht\n");

        return NULL; 
    }

    //! sliding window daten
    else if (req->ReqType == ReqData)
    {
        //! timer initialisieren (beim ersten paket)
        if (packetCount == 0) 
        {
            startTime = getCurrentTime();
        }

        //! äußere schleife um warten und acks zu sammeln / resenden
        while(1)
        {
            //! status update
            if (windowFull) 
            {
                *windowFull = (packetCount >= winSize);
            }
            
            currentTime = getCurrentTime();

            //! prüfen ob "großes" timeout für retransmission erreicht
            if (packetCount > 0 && (currentTime - startTime) > timeout) 
            {
                printf("client: TIMEOUT base %lu. starte einzel-retransmit\n", baseNum);

                //! go back n: nur das älteste Paket (baseNum) senden (einzeln retransmission)
                int idx = (baseNum - 1) % MAX_WINDOW_SIZE;
                
                ssize_t res = sendto(socketFd, 
                                     &windowBuffer[idx], 
                                     sizeof(struct request), 
                                     0, 
                                     (struct sockaddr*)&serverAddr, 
                                     serverAddrLen);
                
                if (res < 0) 
                {
                    perror("client: ERROR resend");
                }
                else 
                {
                    printf("client: RESEND paket %lu\n", baseNum);
                }

                //! timer neustarten
                startTime = getCurrentTime();

                //!time slot simulieren
                struct timeval sleepTime;
                sleepTime.tv_sec = 0;
                sleepTime.tv_usec = timeSlot;
                select(0, NULL, NULL, NULL, &sleepTime);

                continue; 
            }

            //! warten auf ACK oder ablauf des time slots (1 durchlauf)
            fd_set socketDataFds; //! sockets die überwacht werden sollen (für select)
            
            FD_ZERO(&socketDataFds); //! liste leeren
            FD_SET(socketFd, &socketDataFds); //! offenes socket hinzufügen
            
            struct timeval tv;
            tv.tv_sec = 0; 
            tv.tv_usec = timeSlot;

            //! schlafen bis daten oder timout (100ms)
            int retval = select(socketFd + 1, &socketDataFds, NULL, NULL, &tv);

            if (retval == -1) 
            {
                perror("client: ERROR select\n");
                return NULL;
            }
            else if (retval > 0 && FD_ISSET(socketFd, &socketDataFds)) //! daten bereit
            {
                addrLen = sizeof(senderAddr);
                memset(&ack, 0, sizeof(ack)); //! speicher auf 0

                //! daten lesen
                numBytes = recvfrom(socketFd, &ack, sizeof(ack), 0, 
                                    (struct sockaddr*)&senderAddr, &addrLen);

                //! antwort / ack erhalten
                if (numBytes > 0 && ack.AnswType == AnswOk) 
                {
                    //! prüfen ob ack größer als linker fensterrand
                    if (ack.SeNo >= baseNum)
                    {
                        printf("client: ack erhalten %lu (fenster slidet)\n", ack.SeNo);
                        
                        //! anzahl bestätigster pakete
                        unsigned long confirmed = (ack.SeNo - baseNum) + 1;
                        
                        //! fenster und zähler aktualisieren
                        baseNum = ack.SeNo + 1;
                        packetCount -= confirmed;
                        
                        startTime = getCurrentTime();
                    }
                }
            }

            if (packetCount < winSize )
            {
                break;
            }
        } 

        if (windowFull)
        {
            *windowFull = 0;
        }
        
        //! fenster nicht voll, platz berechnen
        int bufferIndex = (nextNum - 1) % MAX_WINDOW_SIZE;
        
        memcpy(&windowBuffer[bufferIndex], req, sizeof(struct request));
        
        struct request *reqPtr = &windowBuffer[bufferIndex];
        reqPtr->SeNr = nextNum; //! aktuelle paket nummer setzen
        
        printf("client: sende paket nr. %lu (anzahl: %d)\n", reqPtr->SeNr, packetCount + 1);

        //! daten senden
        numBytes = sendto(socketFd, reqPtr, sizeof(struct request), 0, 
                          (struct sockaddr*)&serverAddr, serverAddrLen);
        
        if (numBytes < 0) 
        {
            perror("client: ERROR sendto\n");
            return NULL;
        }

        //!time slot simulieren
        struct timeval sleepTime;
        sleepTime.tv_sec = 0;
        sleepTime.tv_usec = timeSlot;
        select(0, NULL, NULL, NULL, &sleepTime);

        //! variablen nach senden aktualisieren
        if (packetCount == 0) 
        {
            startTime = getCurrentTime();
        }

        packetCount++;
        nextNum++;
        
        return NULL; 
    }

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

    struct request req;
    memset(&req, 0, sizeof(req));   //! speicher auf 0
    req.ReqType = ReqHello; 
    req.SeNr = 0;                   
    req.FlNr = 0;                   

    printf("client: sendet HALLO\n");

    struct answer *ans = doRequest(&req, winSize, NULL, NULL);

    if (ans != NULL) 
    {
        printf("client: HELLO verbindung\n");
        return 0;
    }
    
    perror("client: TIMEOUT hello gescheitert\n");
    return -1;
}

int arqSendData(const struct app_unit *app, int winSize)
{
    /* TODO:
     * - Sliding Window Logik implementieren
     * - ACKs einsammeln (Polling)
     * - Nur Senden, wenn Platz im Fenster ist
     */

    struct request req;
    memset(&req, 0, sizeof(req)); //! speicher auf 0
    req.ReqType = ReqData; //! typ ist DATA
    req.FlNr = app->len; //! bytes an nutzdaten
    memcpy(req.name, app->data, app->len); //! daten kopieren

    doRequest(&req, winSize, NULL, NULL);

    return 0;
}

int arqSendClose(int winSize) 
{
    printf("client: warten auf letzte acks...\n");

    double startTime = getCurrentTime(); 
    double currentTime;
    
    //! läuft solange noch unbestätigte daten da sind
    while (packetCount > 0)
    {
        fd_set socketDataFds; //! sockets die überwacht werden sollen (für select)
        struct timeval tv;

        FD_ZERO(&socketDataFds);
        FD_SET(socketFd, &socketDataFds);

        tv.tv_sec = 0; 
        tv.tv_usec = timeSlot; //! 100ms warten

        //! schlafen bis daten oder timout (100ms)
        //! socket + 1, weil er alle sockets davor prüft 
        //! lässt dockets mit daten in readfds stehen, wird später geprüft
        //! setzt tv zurück
        int retval = select(socketFd + 1, &socketDataFds, NULL, NULL, &tv);

        if (retval == -1) 
        {
            perror("client: ERROR select (close-wait)\n");
            return -1;
        }
        else if (retval == 0) //! timeout (100ms slot vorbei)
        {
            currentTime = getCurrentTime();
            if ((currentTime - startTime) > timeout) //! "großer" timeout, requests neu senden
            {
                printf("client: TIMEOUT in close-phase (ab %lu neu senden)\n", baseNum);
                unsigned long currentNum = baseNum;
                while (currentNum < nextNum) 
                {
                    int idx = (currentNum - 1) % MAX_WINDOW_SIZE;
                    ssize_t res = sendto(socketFd, &windowBuffer[idx], sizeof(struct request), 0, (struct sockaddr*)&serverAddr, serverAddrLen);
                    
                    if (res < 0) 
                    {
                        perror("client: ERROR close-resend");
                    }

                    currentNum++;
                }
                startTime = getCurrentTime(); 
            }
            continue; 
        }
        else 
        {
            if (FD_ISSET(socketFd, &socketDataFds)) //! von select gesetzt, daten in unserem socket
            {
                struct answer ack;
                struct sockaddr_storage senderAddr;
                socklen_t addrLen = sizeof(senderAddr);
                
                ssize_t numBytes = recvfrom(socketFd, &ack, sizeof(ack), 0, (struct sockaddr*)&senderAddr, &addrLen);

                if (numBytes > 0 && ack.AnswType == AnswOk) 
                {
                    if (ack.SeNo >= baseNum)
                    {
                        printf("client: ack in close-phase %lu\n", ack.SeNo);
                        unsigned long confirmed = (ack.SeNo - baseNum) + 1;
                        baseNum = ack.SeNo + 1;
                        packetCount -= confirmed;
                        
                        startTime = getCurrentTime();
                    }
                }
            }
        }
    }

    //! close senden
    struct request req;
    memset(&req, 0, sizeof(req));
    req.ReqType = ReqClose;
    req.SeNr = 0;
    req.FlNr = 0;

    printf("client: sende close (buffer leer)\n");

    struct answer *ans = doRequest(&req, winSize, NULL, NULL);

    if (ans != NULL && ans->AnswType == AnswOk) 
    {
        printf("client: close bestaetigt\n");
        return 0;
    }

    perror("client: TIMEOUT close (oder abgelehnt)\n");
    return -1;
}

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

//! RINGPUFFER: 
//! Index=(Sequenznummer−1)%MAX_SIZE
#define MAX_WINDOW_SIZE 10 //! sliding window buffer size
static struct request windowBuffer[MAX_WINDOW_SIZE]; //! speicher für gesendete pakete
static int packetCount = 0; //! zählen wieviele pakete unterwegs

static unsigned long baseNum = 1; //! altestes unbestätigstes Paket (linker Fensterrand)
static unsigned long nextNum = 1; //! nächstes neues Paket (rechter Fensterrand)

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
    ssize_t numBytes;
    socklen_t addrLen;
    struct sockaddr_storage senderAddr;
    static struct answer ack; 

    double startTime;
    double currentTime;
    const double timeout = 3.0; 

    //! hello / close logik
    if (req->ReqType == ReqHello || req->ReqType == ReqClose)
    {
        int versuche = 0;
        const int maxVersuche = (req->ReqType == ReqHello) ? 5 : 3;

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
                perror("client sendto error");
                return NULL;
            }

            //! sendezeit merken
            startTime = getCurrentTime();

            //! innere schleife, auf antwort warten
            while(1)
            {
                addrLen = sizeof(senderAddr);
                memset(&ack, 0, sizeof(ack)); 
                
                numBytes = recvfrom(socketFd, &ack, sizeof(ack), 0, 
                                    (struct sockaddr*)&senderAddr, &addrLen);

                //! antwort erhalten
                if (numBytes > 0)
                {
                    if (ack.AnswType == AnswHello || ack.AnswType == AnswOk) 
                    {
                        return &ack; 
                    } 
                }

                //! keine antwort erhalten
                if (numBytes < 0) 
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) 
                    {
                        //! timeout prüfen
                        currentTime = getCurrentTime();
                        if ((currentTime - startTime) > timeout) 
                        {
                            printf("client timeout, versuch %d...\n", versuche);
                            break; //! aus schleife austreten / neu senden
                        }
                        continue; //! timeout nochnicht erroicht
                    } 
                    else 
                    {
                        perror("client recvfrom error"); 
                        return NULL;
                    }
                }
            } 
        } while (versuche < maxVersuche); 
        
        return NULL; 
    }

    //! sliding window daten
    else if (req->ReqType == ReqData)
    {
        //! äußere schleife um warten und acks zu sammeln
        while(1)
        {
            //! prüfen ob platz imfesnter
            if (packetCount < winSize)
            {
                break; //! Platz vorhanden, weiter zum Senden
            }
            
            if (windowFull) 
            {
                *windowFull = 1;
            }

            //! fenster voll, warten timer
            startTime = getCurrentTime();

            //! innere schleife zum warten auf antwort (Polling)
            while(1)
            {
                addrLen = sizeof(senderAddr);
                memset(&ack, 0, sizeof(ack)); 

                numBytes = recvfrom(socketFd, &ack, sizeof(ack), 0, 
                                    (struct sockaddr*)&senderAddr, &addrLen);

                //! antwort / ack erhalten
                if (numBytes > 0 && ack.AnswType == AnswOk) 
                {
                    //! prüfen ob ack größer als linker fensterrand
                    if (ack.SeNo >= baseNum)
                    {
                        printf("client ack erhalten %lu (fenster slidet)\n", ack.SeNo);
                        
                        //! anzahl bestätigster poakete
                        unsigned long confirmed = (ack.SeNo - baseNum) + 1;
                        
                        //! fenster und zähler aktualisieren
                        baseNum = ack.SeNo + 1;
                        packetCount -= confirmed;
                        
                        break; //! wieder platz im fesnter
                    }
                }

                //! keine antwort (oder fehler)
                if (numBytes < 0) 
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        currentTime = getCurrentTime(); 
                        if ((currentTime - startTime) > timeout) 
                        {
                            printf("client timeout base %lu. starte go-back-n\n", baseNum);

                            //! GO-BACK-N: von base zu current alle Pakete neu senden
                            unsigned long currentNum = baseNum;
                            while (currentNum < nextNum) 
                            {
                                int idx = (currentNum - 1) % MAX_WINDOW_SIZE;
                                
                                ssize_t res = sendto(socketFd, 
                                                     &windowBuffer[idx], 
                                                     sizeof(struct request), 
                                                     0, 
                                                     (struct sockaddr*)&serverAddr, 
                                                     serverAddrLen);
                                if (res < 0) perror("client error resend");
                                else printf("client resend paket %lu\n", currentNum);

                                currentNum++;
                            }
                            
                            //! timeout neu starten
                            startTime = getCurrentTime();
                            continue; 
                        }
                        continue;
                    } 
                    else 
                    {
                        perror("client recvfrom error");
                        return NULL;
                    }
                }
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
        
        printf("client sende paket nr. %lu (anzahl: %d)\n", reqPtr->SeNr, packetCount + 1);

        //! daten senden
        numBytes = sendto(socketFd, reqPtr, sizeof(struct request), 0, 
                          (struct sockaddr*)&serverAddr, serverAddrLen);
        
        if (numBytes < 0) 
        {
            perror("cleint error sendto");
            return NULL;
        }

        //! variablen nach sendne aktualisieren
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

    printf("client sendet HALLO\n");

    struct answer *ans = doRequest(&req, winSize, NULL, NULL);

    if (ans != NULL) 
    {
        printf("client HELLO verbindung\n");
        return 0;
    }
    
    fprintf(stderr, "client hello gescheitert (timeout)\n");
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
    memcpy(req.name, app->data, app->len); //! daten in "name" kopieren

    doRequest(&req, winSize, NULL, NULL);

    return 0;
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
    memset(&req, 0, sizeof(req));
    req.ReqType = ReqClose;
    req.SeNr = 0;
    req.FlNr = 0;

    printf("client sende close\n");

    struct answer *ans = doRequest(&req, winSize, NULL, NULL);

    if (ans != NULL && ans->AnswType == AnswOk) {
        printf("client close bestätigt\n");
        return 0;
    }

    fprintf(stderr, "client close timeout oder abgelehnt\n");
    return -1;
}

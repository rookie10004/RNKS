/* serverSy.c - UDP + ARQ/Go-Back-N-Server mit Loss-Simulation
 *
 * Schichten:
 *   - SAP-Schicht (UDP): initServer, getRequest, sendAnswer, exitServer
 *   - ARQ-Schicht: processRequest(), arqServerLoop()
 *
 * Die Anwendung (Datei öffnen/schreiben/schließen) wird über Callbacks
 * aus server.c angebunden:
 *   appStartFn  appStartTransfer
 *   appWriteFn  appWriteData
 *   appEndFn    appEndTransfer
 *
 * WICHTIG:
 *   - Dateiname und Funktionssignaturen in serverSy.h sollen
 *     unverändert beibehalten werden.
 */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "data.h"
#include "config.h"
#include "serverSy.h"

/* Optionale globale Variablen:
 *   - Socket-Deskriptor
 *   - zuletzt bekannte Client-Adresse (für sendto)
 *   - nächst erwartete Sequenznummer (nextExpected)
 *   - Zeiger auf die Anwendungscallbacks
 */

struct sockaddr_storage clientAddr; //! wer hat gesendet
socklen_t clientAddrLen = 0;        
int socketFd = -1;                    //!socket file descriptor

//! callbacks für process request / function pointer
static appStartFn appStartCallback = NULL;
static appWriteFn appWriteCallback = NULL;
static appEndFn appEndCallback = NULL;

/* --------------------------------------------------------------- */
/*  SAP-Schicht (UDP)                                              */
/* --------------------------------------------------------------- */

int initServer(const char *port)
{
    /* TODO:
     *  - mit getaddrinfo(NULL, port, ...) eine lokale Adresse für UDP/IPv6
     *    ermitteln
     *  - mit socket(...) einen UDP/IPv6-Socket erzeugen
     *  - mit bind(...) an die Adresse binden
     *  - Socket-Deskriptor in globaler Variable speichern
     *  - bei Erfolg 0, bei Fehler <0 zurückgeben
     */

    struct addrinfo hints; //! anfrage formular
    struct addrinfo* result; //! suchergebnis (liste)
    
    //! adress suche
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET6;    //! ipv6
    hints.ai_socktype = SOCK_DGRAM; //! udp (datagram)
    hints.ai_flags = AI_PASSIVE;   //! passiv (daten empfangen)
    
    //!eigene Adressen suchen / hören
    int status;
    status = getaddrinfo(NULL, port, &hints, &result); //! NULL weil wir keine fremde IP suchen, (::1 loopback) jedes Paket nehmen
    if (status != 0) 
    {
        fprintf(stderr, "server getaddrinfo error\n");
        return -1;
    }

    //! liste durchgehen und binden
    struct addrinfo* current;
    for (current = result; current != NULL; current = current->ai_next) 
    {
        socketFd = socket(current->ai_family, current->ai_socktype, current->ai_protocol); //! socket erstellen / öffnen
        if (socketFd == -1) //! nächster eintrag, bis erfolgreich
        {

            continue; 
        }

        if (bind(socketFd, current->ai_addr, current->ai_addrlen) == 0) //! adresse binden (socket zum daten bereitstellen)
        {
            break; 
        }

        close(socketFd);
    }

    freeaddrinfo(result); //! ergebnis liste freen

    if (current == NULL)  //! testen ob überhaupt ein socket erstellt wurde oder (Ende der Liste erreicht)
    {
        fprintf(stderr, "server kein socket für %s\n", port);
        return -1;
    }

    printf("server init für %s\n", port);

    return 0;
}

struct request *getRequest(void)
{
    /* TODO:
    *  - mit recvfrom(...) ein Request-Paket vom Socket lesen
    *  - die Adresse des Clients (struct sockaddr_storage) merken,
    *    damit sendAnswer() dorthin antworten kann
    *  - bei Erfolg &req zurückgeben
    *  - bei Fehler oder wenn keine Daten vorliegen: NULL zurückgeben
    */

    static struct request req; //! zu empfangende request
    
    ssize_t numBytes;

    clientAddrLen = sizeof(clientAddr);

    //! auf paket warten (!blocking!)
    numBytes = recvfrom(socketFd, 
                        &req, //! gesendette bytes werden geschrieben
                        sizeof(req), 
                        0, 
                        (struct sockaddr*)&clientAddr, //!absender adresse wird geschrieben
                        &clientAddrLen);

    if (numBytes < 0) //! testen wieviele bytes empfangen
    {
        perror("server recvfrom error"); 
        return NULL; 
    }

    return &req; //! adresse des pakets zurückgeben
}

int sendAnswer(struct answer *answerPtr)
{
    /* TODO:
     *  - mit sendto(...) eine Antwort an die zuletzt bekannte
     *    Client-Adresse schicken
     *  - bei Erfolg 0, bei Fehler <0 zurückgeben
     */

     ssize_t numBytes;

    //! antwort zurückschicken 
    numBytes = sendto(socketFd, 
                      answerPtr,
                      sizeof(struct answer), 
                      0, 
                      (struct sockaddr*)&clientAddr,
                      clientAddrLen);

    if (numBytes < 0) //! testen wieviele bytes gesendet
    {
        perror("server sendAnswer error");
        return -1;
    }

    return 0;
}

int exitServer(void)
{
    /* TODO:
     *  - Socket schließen (close)
     *  - globale Zustandsvariablen zurücksetzen
     */
    
    //! offenes socket schließen
    if (socketFd >= 0) 
    {
        close(socketFd); //! bind am port auch entfernt

        socketFd = -1;
    }

    return 0;
}

/* --------------------------------------------------------------- */
/*  ARQ-/GBN-Logik (Empfänger)                                     */
/* --------------------------------------------------------------- */

/*
 * processRequest:
 *  - nimmt ein Request-Paket entgegen
 *  - führt die ARQ-/GBN-Empfangslogik aus
 *  - erzeugt eine passende Antwort (ACK/Fehler)
 *
 *   ReqHello:
 *     - Sequenznummernzustand initialisieren (z.B. nextExpected = 0)
 *     - Anwendung per appStartFn informieren
 *     - eine passende Antwort (AnswHello/AnswOk) eintragen
 *
 *   ReqData:
 *         * ggf. Nutzdaten an appWriteFn übergeben
 *         * (kumulatives) ACK senden 
 *       
 *   ReqClose:
 *     - appEndFn aufrufen
 *     - Abschluss-ACK senden
 *
 * lossReq:
 *   - simulierte Paketverlustrate für Requests (0.0..1.0)
 *     (z.B. über Zufallszahlvergleich ein Paket "fallen lassen")
 *
 * Rückgabewert:
 *   - Zeiger auf ausgefüllte Antwortstruktur (answPtr)
 *   - NULL, wenn das Request-Paket vollständig verworfen wurde
 */

static struct answer *processRequest(struct request *reqPtr,
                                     struct answer *answPtr,
                                     double lossReq)
{
    /* TODO:
     * - Paketverlust über lossReq simulieren
     * - je nach ReqType (ReqHello / ReqData / ReqClose) handeln
     * - Antwort (AnswType, SeNo/ErrNo) setzen
     * - bei verworfenem Request NULL zurückgeben
     */

    //! welches paket als nächstes erwartet
    static unsigned long nextExpectedSeNr = 1;

    double randomNum = (double)rand() / (double)RAND_MAX; //! zahl zwischen 0 und 1
    if (lossReq > 0.0 && randomNum < lossReq) //! paketverlust simulieren
    {
        printf("server paketverlust\n");
        return NULL; 
    }

    if (reqPtr == NULL) //! test ob reqPtr gültig
    {
        return NULL;
    }

    memset(answPtr, 0, sizeof(struct answer)); //! speicher auf 0

    //! reqest auswerten
    switch (reqPtr->ReqType) 
    {
        //! neue verbindung
        case ReqHello:
            printf("server HELLO empfangen\n");
            
            //! erwartetes paket zurücksetzen
            nextExpectedSeNr = 1;

            //! datei / app starten (callback)
            if (appStartCallback() < 0) 
            {
                answPtr->AnswType = AnswErr; 
            } 
            else 
            {
                answPtr->AnswType = AnswOk;  
                answPtr->SeNo = 0; 
            }
            break;

        case ReqData:
            
            //! prüfen ob duplikate
            if (reqPtr->SeNr == nextExpectedSeNr) //! erwartetes paket erhalten
            {
                //! daten schreiben (callabck)
                if (appWriteCallback(reqPtr->name, reqPtr->FlNr) < 0) 
                {
                    printf("server error schreiben\n");
                    answPtr->AnswType = AnswErr;
                } 
                else 
                {
                    answPtr->AnswType = AnswOk;  
                    answPtr->SeNo = reqPtr->SeNr;
                    
                    nextExpectedSeNr++; //! nächstes paket erwarten
                }
            }
            else if (reqPtr->SeNr < nextExpectedSeNr) //! bereits geschriebenes daten erhalten
            {
                printf("server duplikat %lu erhalten\n", reqPtr->SeNr);
                
                //! nichts schreiben, aber ok / ack senden
                answPtr->AnswType = AnswOk;
                answPtr->SeNo = reqPtr->SeNr;
            }
            else //! pakwet aus zukunft
            {
                return NULL;
            }
            break;
            
        case ReqClose:
            printf("server close empfangen \n");
            
            //! datei schließen (callback)
            appEndCallback();
            
            answPtr->AnswType = AnswOk;
            break;

        default:
            return NULL;
    }

    return answPtr;
}

/* --------------------------------------------------------------- */
/*  ARQ-Server-Hauptschleife                                       */
/* --------------------------------------------------------------- */

int arqServerLoop(const char *port,
                  double lossReq,
                  double lossAck,
                  appStartFn appStart,
                  appWriteFn appWrite,
                  appEndFn appEnd)
{
    /* TODO:
     *  - Callbacks in globalen Variablen speichern oder für Weiterreichen 
	 *    Parameterliste in processRequest erweitern
     *  - initServer(port) aufrufen
     *  - Sequenznummerzustand initialisieren (z.B. nextExpected = 0)
     *  - Endlosschleife:
	 *		 * Paketverlust mit lossReq simulieren
     *       * ggf. getRequest() aufrufen
     *       * processRequest(..) aufrufen
     *       * ACK-Verlust mit lossAck simulieren
     *       * bei "kein ACK-Verlust": sendAnswer(..) aufrufen
     *  - exitServer() im Fehlerfall oder bei Beenden verwenden
     */

    appStartCallback = appStart;
    appWriteCallback = appWrite;
    appEndCallback   = appEnd;

    struct request* req; //! zu bekommende request
    static struct answer answ; //! answer
    struct answer* answPtr; //! fertige answer

    if (initServer(port) != 0) //! kein port --> beenden
    {
        return -1;
    }

    printf("server wartet auf pakete\n");

    while (1) 
    {
        req = getRequest(); //! warten bis paket vom client
        
        if (req == NULL) //! testen ob request leer
        {
            continue;
        }

        answPtr = processRequest(req, &answ, lossReq); //! paket verarbeiten und answer formulieren

        if (answPtr == NULL) //! request verlust simuliert
        {
            continue;
        }

        double randomNum = (double)rand() / (double)RAND_MAX; //! zahl zwischen 0 und 1
        if (lossAck > 0.0 && randomNum < lossAck) //! ack verlust simulieren
        {
            printf("server ack verlust\n");
            continue;
        }

        sendAnswer(answPtr); //! sende antwort
    }

    exitServer();
    return 0;
}

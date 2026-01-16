#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "data.h"
#include "config.h"
#include "clientSy.h"

/* usage-Ausgabe */
static void usage(const char *progName)
{
    fprintf(stderr, "Usage: %s -a <server> -p <port> -f <file> -w <window>\n", progName);
    fprintf(stderr, "       -a <server> : Server-Adresse (Default: %s)\n",
            (DEFAULT_SERVER == NULL) ? "loopback" : DEFAULT_SERVER);
    fprintf(stderr, "       -p <port>   : Server-Port (Default: %s)\n", DEFAULT_PORT);
    fprintf(stderr, "       -f <file>   : Eingabedatei\n");
    fprintf(stderr, "       -w <window> : Fenstergröße (1..10)\n");
    exit(EXIT_FAILURE);
}

/* Eine Zeile aus Datei in app_unit einlesen (Begrenzung BufferSize).
 *
 * Signatur ist vorgegeben und soll beibehalten werden.
 *
 * - Rückgabewerte:
 *        > 1 : es wurden Nutzdaten gelesen
 *        > 0 : EOF (keine Daten mehr)
 *        < 0 : Fehler (z.B. ferror(f) != 0)
 */

static int readAppUnit(struct app_unit *app, FILE *f)
{
    if (f == NULL || app == NULL) //! prüfen ob datei leer
    {
        return -1;
    }
    
    memset(app->data, 0, BufferSize); //! speicher auf 0
    app->len = 0;
    
    size_t itemsRead; //! wieviel bytes wirklich gelesen 
    itemsRead = fread(app->data, 1, BufferSize, f); //! 512 bytes lesen von file in daten

    if (itemsRead > 0) //! daten erfolgreich gelesen
    {
        app->len = itemsRead;
        return 1;
    }

    if (feof(f)) //! ende der datei erreicht
    {
        return 0;
    }

    perror("client error datei lesen");
    return -1;
}

int main(int argc, char *argv[])
{
    const char *server     = DEFAULT_SERVER;
    const char *filename   = NULL;
    const char *port       = DEFAULT_PORT;
    const char *windowSize = "1";

    FILE *fp = NULL;
    long i;

    /* Kommandozeilenparameter auswerten */
    if (argc > 1) {
        for (i = 1; i < argc; i++) {
            if (((argv[i][0] == '-') || (argv[i][0] == '/')) &&
                (argv[i][1] != 0) && (argv[i][2] == 0)) {

                switch (tolower((unsigned char)argv[i][1])) {

                case 'a': /* Server-Adresse */
                    if (argv[i + 1] && argv[i + 1][0] != '-') {
                        server = argv[++i];
                        break;
                    }
                    usage(argv[0]);
                    break;

                case 'p': /* Server-Port */
                    if (argv[i + 1] && argv[i + 1][0] != '-') {
                        port = argv[++i];
                        break;
                    }
                    usage(argv[0]);
                    break;

                case 'f': /* Eingabedatei */
                    if (argv[i + 1] && argv[i + 1][0] != '-') {
                        filename = argv[++i];
                        break;
                    }
                    usage(argv[0]);
                    break;

                case 'w': /* Fenstergröße */
                    if (argv[i + 1] && argv[i + 1][0] != '-') {
                        windowSize = argv[++i];
                        break;
                    }
                    usage(argv[0]);
                    break;

                default:
                    usage(argv[0]);
                    break;
                }
            } else {
                usage(argv[0]);
            }
        }
    }

    if (!filename) {
        usage(argv[0]);
    }

    /* TODO:
     *   - Datei filename zum Lesen öffnen (z.B. fopen)
     *   - bei Fehler: perror / Fehlermeldung und EXIT_FAILURE
     *   - bei Erfolg: FILE* in fp ablegen
     */

    
    fp = fopen(filename, "rb"); //! datei in binary read öffnen
    if (fp == NULL) 
    {
        perror("client: ERROR datei oeffnen\n");
        exit(EXIT_FAILURE);
    }

    printf("client: sending file '%s'\n", filename);

    /* ARQ-Client initialisieren */
    initClient((char *)server, port);

    /* Hello/Verbindungsaufbau */
    if (arqSendHello(atoi(windowSize)) != 0) {
        perror("client: ERROR hello failed, aborting\n");
        /* TODO: Datei ggf. schließen, falls sie bereits geöffnet wurde */
        closeClient();
        return EXIT_FAILURE;
    }

    /* Datei -> zeilenweise lesen und jede Zeile als app_unit an 
	 * arqSendData() übergeben 
     *
     *   - Fehlerfall (readAppUnit(..) < 0) behandeln
     */
    
    struct app_unit app; //! app zu senden
    unsigned long packetCounter = 0; //! anzahl gesendete pakete
    
    int res;
    while ((res = readAppUnit(&app, fp)) > 0) //! solange wie daten gelesen werden
    {
        packetCounter++;
        
        //! gelesene app + fenstergröße senden
        if (arqSendData(&app, atoi(windowSize)) != 0) //! solange wie senden erfolgreich
        {
            fprintf(stderr, "client: ERROR senden %lu\n", packetCounter);
            closeClient(); 

            if (fp) //! file pointer schließen
            {
                fclose(fp);
                return EXIT_FAILURE;
            }
        }
    }

    //! prüfen ob erfolgreich gelesen / verpackt
    if (res < 0) 
    {
        perror("client: ERROR lesefehler\n");
    } 
    else 
    {
        printf("client: datei erfolgreich gesendet (%lu pakete)\n", packetCounter);
    }
    

    /* Close / Verbindungsabbau */
    if (arqSendClose(atoi(windowSize)) != 0) 
    {
        perror("client: ERROR while sending close\n");
    }

    /* TODO:
     *   - geöffnete Datei wieder schließen
     */
    closeClient();

    return EXIT_SUCCESS;
}

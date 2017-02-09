using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RemoteObjects;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.Diagnostics;

namespace Subscriber
{
    class Subscriber
    {
        private subscriberObject objetoSubscriber = null;
        public delegate void delRegistoPublisher(string url);
        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;
        TcpChannel channel = null;

        static void Main(string[] args)
        {
            Console.WriteLine(args[0] + " criado com sucesso");
            string[] url = args[1].Split(':'); //separamos para podermos obter o porto para inicializar o canal 
            string[] port = url[2].Split('/'); //temos que voltar a separar para obter finalmente o porto que se encontra em port[0]
            string[] portFinal = port[0].Split('/');

            Subscriber p = new Subscriber();

            p.inicialização(args[0], Int32.Parse(portFinal[0]), args[1], args[2], args[3], args[4], args[5]); //[0} = id [1] = urlPublisher, [2]=urlBroker1 [3] = urlBroker2 [4]=urlBroker [5] = urlPuppet

            Console.ReadLine();
        }

        public void inicialização(string id, int port, string url, string urlBroker, string urlBroker2, string urlBroker3, string urlPuppet)
        {
            channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, false);
            objetoSubscriber = new subscriberObject(id,url,urlBroker,urlBroker2, urlBroker3, port, urlPuppet);
            RemotingServices.Marshal(objetoSubscriber, "sub", typeof(subscriberObject));

            IBroker objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBroker);
            funcaoCallBack = new AsyncCallback(OnExit);
            delRegistoPublisher del = new delRegistoPublisher(objetoBroker.addListaSubscriber);
            ar = del.BeginInvoke(url, funcaoCallBack, null);

            objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBroker2);
            funcaoCallBack = new AsyncCallback(OnExit);
            delRegistoPublisher del1 = new delRegistoPublisher(objetoBroker.addListaSubscriber);
            ar = del1.BeginInvoke(url, funcaoCallBack, null);

            objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBroker3);
            funcaoCallBack = new AsyncCallback(OnExit);
            delRegistoPublisher del2 = new delRegistoPublisher(objetoBroker.addListaSubscriber);
            ar = del2.BeginInvoke(url, funcaoCallBack, null);
        }

        public void OnExit(IAsyncResult ar)
        {
            delRegistoPublisher del = (delRegistoPublisher)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
    }

    public class subscriberObject : MarshalByRefObject, ISubscriber
    {
        private string url; //url do subscriber para enviar para o broker para este saber que tem de enviar eventos para este subscriber
        private string urlBroker1;
        private string urlBroker2;
        private string urlBroker3;
        private string id; //id do subscriber 
        private string urlPuppet;
        private int port;

        public bool sentinela = false;
        public bool sentinela2 = false;

        private int identifier = 1;

        bool freezeAtivo = false;
        static public Object lockSentinela = new Object();

        private bool podeEnviar = false;

        //Variaveis novas
        int sequencer = 0;

        

        //base para chamadas assíncronas
        public delegate void delTrataSubscricao(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento);
        public delegate void delACK(Evento evento, string funcaoAInvocar);
        public delegate void delRecallFunc(string comando, Evento evento, bool freeze);
        public delegate void delReenviaSub(string comando, Evento evento);
        public delegate void delSuspeitoDetetado(string urlSuspeito, string url, string urlNovoLider);


        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;

        private List<Evento> listaEventosEsperaACK = new List<Evento>(); // esta lista contém os eventos à espera do ACK do broker que foram enviados com sucesso
        private List<Evento> listaEventosEsperaACK2 = new List<Evento>(); //para os unsubscribes
        private Dictionary<string, string> listaBrokers = new Dictionary<string, string>();

        public subscriberObject(string id,string url, string urlBroker, string urlBroker2, string urlBroker3,  int port, string urlPuppet)
        {
            this.id = id;
            this.url = url;
            this.urlBroker1 = urlBroker;
            this.port = port;
            this.urlPuppet = urlPuppet;
            this.urlBroker2 = urlBroker2;
            this.urlBroker3 = urlBroker3;

            listaBrokers.Add(urlBroker1, "ATIVO"); //este broker fica predefinido como o primário identificado pelo estado ATIVO , os restantes ficam como backup, no estado ESPERA
            listaBrokers.Add(urlBroker2, "ESPERA");
            listaBrokers.Add(urlBroker3, "ESPERA");
        }

        public void recebeComando(string comando, Evento evento, bool freeze) //recebe comandos do PM
        {

            if (evento == null) //se no evento vier nada entao é um comando de freeze
            {
                lock(this)
                {
                    if (!freeze) //se vier a false, entao vem o unfreeze
                    {
                        freezeAtivo = false;
                        Monitor.PulseAll(this);
                    }
                    else if (freeze)
                    {
                        freezeAtivo = true;
                    }
                }
            }
            else //se for um comando que nao seja freeze
            {
                lock (this)
                {
                    if (freezeAtivo)
                        Monitor.Wait(this);
                }

                KeyValuePair<string,string> urlBrokerAtivo = listaBrokers.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo

                IBroker objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);
                funcaoCallBack = new AsyncCallback(OnExit);

                evento.setIdSubscriber(id);

                if (comando.Equals("Subscribe"))
                {
                    lock(this)
                    {
                        evento.setIdentificador(identifier);
                        identifier++;

                        listaEventosEsperaACK.Add(evento);
                    }

                    delACK del1 = new delACK(verificaEntregaCompleta);
                    funcaoCallBack = new AsyncCallback(OnExit2);
                    ar = del1.BeginInvoke(evento,"subscribe", funcaoCallBack, null);

                    evento.setQuemEnviouEvento("subscriber");
                    evento.setComando(comando);

                    try
                    {
                        delTrataSubscricao del = new delTrataSubscricao(objetoBroker.registoSubscriber);
                        funcaoCallBack = new AsyncCallback(OnExit);
                        ar = del.BeginInvoke(id, url, comando, evento, url, funcaoCallBack, null);
                    }catch(Exception e) { }
    
                }
                else if (comando.Equals("Unsubscribe"))
                {
                    lock (this)
                    {
                        evento.setIdentificador(identifier);
                        identifier++;

                        listaEventosEsperaACK2.Add(evento);
                    }

                    delACK del1 = new delACK(verificaEntregaCompleta);
                    funcaoCallBack = new AsyncCallback(OnExit2);
                    ar = del1.BeginInvoke(evento, "unsubscribe", funcaoCallBack, null);

                    evento.setQuemEnviouEvento("subscriber");
                    evento.setComando(comando);
                    try
                    {
                        delTrataSubscricao del = new delTrataSubscricao(objetoBroker.unsubscribe);
                        funcaoCallBack = new AsyncCallback(OnExit);
                        ar = del.BeginInvoke(id, url, comando, evento, url, funcaoCallBack, null);
                    }
                    catch(Exception e) { }

                   
                 }
                
            }
        }
       //função é responsável por ver se é necessário reenviar o evento ou nao
        public void verificaEntregaCompleta(Evento evento,string funcaoAInvocar)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }
            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           
            lock (lockSentinela)
            {
                if (funcaoAInvocar.Equals("subscribe"))
                {
                    if (listaEventosEsperaACK.Any(x => x.getIdentificador() == evento.getIdentificador()))
                    {
                        if (!sentinela)
                        {
                            sentinela = true;

                            KeyValuePair<string, string> urlBrokerAtivo = listaBrokers.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                            listaBrokers[urlBrokerAtivo.Key] = "SUSPEITO";

                            KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokers.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                            listaBrokers[urlNovoBrokerAtivo.Key] = "ATIVO";

                            try
                            {

                                IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                delSuspeitoDetetado del = new delSuspeitoDetetado(brok.detetaSuspeito);
                                funcaoCallBack = new AsyncCallback(OnExit3);
                                ar = del.BeginInvoke(urlBrokerAtivo.Key, url, urlNovoBrokerAtivo.Key, funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        delReenviaSub del1 = new delReenviaSub(reenviaSub);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        ar = del1.BeginInvoke(evento.getComando(), evento, funcaoCallBack, null);

                    }
                }
                else if(funcaoAInvocar.Equals("unsubscribe"))
                {
                    if (listaEventosEsperaACK2.Any(x => x.getIdentificador() == evento.getIdentificador()))
                    {
                        if (!sentinela)
                        {
                            sentinela = true;

                            KeyValuePair<string, string> urlBrokerAtivo = listaBrokers.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                            listaBrokers[urlBrokerAtivo.Key] = "SUSPEITO";

                            KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokers.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                            listaBrokers[urlNovoBrokerAtivo.Key] = "ATIVO";

                            try
                            {

                                IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                delSuspeitoDetetado del = new delSuspeitoDetetado(brok.detetaSuspeito);
                                funcaoCallBack = new AsyncCallback(OnExit3);
                                ar = del.BeginInvoke(urlBrokerAtivo.Key, url, urlNovoBrokerAtivo.Key, funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        delReenviaSub del1 = new delReenviaSub(reenviaSub);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        ar = del1.BeginInvoke(evento.getComando(), evento, funcaoCallBack, null);

                    }
                }
                else
                {
                }
            }
        }

        public void reenviaSub(string comando, Evento evento)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            KeyValuePair<string, string> urlBrokerAtivo = listaBrokers.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo

            IBroker objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);
            funcaoCallBack = new AsyncCallback(OnExit);

            if (comando.Equals("Subscribe"))
            {
                try
                {
                    delTrataSubscricao del = new delTrataSubscricao(objetoBroker.registoSubscriber);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    ar = del.BeginInvoke(id, url, comando, evento, url, funcaoCallBack, null);
                }
                catch (Exception e) { }

                Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           

                lock (lockSentinela)
                {
                    if (listaEventosEsperaACK.Any(x => x.getIdentificador() == evento.getIdentificador()))
                    {
                        if (!sentinela2)
                        {
                            sentinela = false;
                            sentinela2 = true;
                        }
                        delACK del1 = new delACK(verificaEntregaCompleta);
                        funcaoCallBack = new AsyncCallback(OnExit2);
                        ar = del1.BeginInvoke(evento, "subscribe", funcaoCallBack, null);
                    }
                    else
                    {
                        if (listaBrokers.Any(x => x.Value.Equals("ESPERA")))
                        {
                            sentinela2 = false;
                        }
                    }
                }
            }
            else if (comando.Equals("Unsubscribe"))
            {
                try
                {
                    delTrataSubscricao del = new delTrataSubscricao(objetoBroker.unsubscribe);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    ar = del.BeginInvoke(id, url, comando, evento, url, funcaoCallBack, null);
                }
                catch (Exception e) { }

                Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           

                lock (lockSentinela)
                {
                    if (listaEventosEsperaACK2.Any(x => x.getIdentificador() == evento.getIdentificador()))
                    {
                        if (!sentinela2)
                        {
                            sentinela = false;
                            sentinela2 = true;
                        }
                        delACK del1 = new delACK(verificaEntregaCompleta);
                        funcaoCallBack = new AsyncCallback(OnExit2);
                        ar = del1.BeginInvoke(evento,"unsubscribe", funcaoCallBack, null);
                    }
                    else
                    {
                        if (listaBrokers.Any(x => x.Value.Equals("ESPERA")))
                        {
                            sentinela2 = false;
                        }
                    }
                }
            }
        }

        public void recebeACK(int iden) //ACK enviado pelo broker
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            //como o evento foi enviado com sucesso, remove-lo da lista
            lock(this)
                listaEventosEsperaACK.RemoveAll(x => x.getIdentificador() == iden);          
        }

        public void recebeACK2(int iden) //ACK enviado pelo broker
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            //como o evento foi enviado com sucesso, remove-lo da lista
            lock (this)
                listaEventosEsperaACK2.RemoveAll(x => x.getIdentificador() == iden);
        }

        public void recebeEvento(Evento evento, bool freeze) //recebe eventos dos brokers
        {        
            if (evento == null) //se no evento vier nada, entao é um comando de freeze
            {
                lock (this)
                {
                    if (!freeze) //se vier a false, entao vem o unfreeze
                    {
                        freezeAtivo = false;
                        Monitor.PulseAll(this);
                    }
                    else if (freeze)
                    {
                        freezeAtivo = true;
                    }
                }
            }
            else //se for um comando que nao seja freeze
            {
                lock (this)
                {
                    if (freezeAtivo)
                        Monitor.Wait(this);
                }
                Console.WriteLine("Recebido evento de: " + evento.getNomePublisher() + " sobre o topico " + evento.getTopic() + " com o sequence number " + evento.getSeqNumber());

                IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                pm.recebeSubLog(id, evento.getNomePublisher(),evento.getTopic(), evento.getSeqNumber().ToString());
            }

        }

        public void atualizaLider(string urlPrincipal, string urlSuspeito)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);

                lock(lockSentinela)
                     sentinela = true;
                listaBrokers[urlSuspeito] = "SUSPEITO";
                listaBrokers[urlPrincipal] = "ATIVO";
            }
        }

        public void atualizaEstadoBroker(string urlBroker, string estado)
        {
            listaBrokers[urlBroker] = estado;
        }

        public string getBrokerEmEspera(string urlBrokerSuspeito)
        {
            listaBrokers[urlBrokerSuspeito] = "SUSPEITO";

            KeyValuePair<string, string> urlNovo = listaBrokers.First(x => x.Value.Equals("ESPERA")); //vamos buscar o broker que está atualmente ativo
            listaBrokers[urlNovo.Key] = "ATIVO";

            return urlNovo.Key;
        }

        public void recebeStatus()
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            Console.WriteLine(id + " está presente ");
        }

        public string getID()
        {
            return id;
        }

        public string getUrl()
        {
            return url;
        }

        public int getSequencer()
        {
            return sequencer;
        }

        public void incrementSequencer()
        {
            sequencer++;
        }

        public string getUrlBroker()
        {
            KeyValuePair<string, string> urlBrokerAtivo = listaBrokers.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo

            return urlBrokerAtivo.Key;
        }

        public void OnExit(IAsyncResult ar)
        {
            delTrataSubscricao del = (delTrataSubscricao)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        public void OnExit2(IAsyncResult ar)
        {
            delACK del = (delACK)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        public void OnExit3(IAsyncResult ar)
        {
            delRecallFunc del = (delRecallFunc)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        public void OnExit4(IAsyncResult ar)
        {
            delReenviaSub del = (delReenviaSub)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }


    }
}

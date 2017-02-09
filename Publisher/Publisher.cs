using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using RemoteObjects;
using System.Net.Sockets;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace Publisher
{
     class Publisher
    {
        private publisherObject objetoPublisher = null;
        private IBroker objetoBroker = null;
        TcpChannel channel = null;

        //base para chamadas assíncronas
        public delegate void delRegistoPublisher(string url);
        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;

        static void Main(string[] args)
        {
            Console.WriteLine(args[0] + " criado com sucesso " + args[1]);
            string[] url = args[1].Split(':'); //separamos para podermos obter o porto para inicializar o canal 
            string[] port = url[2].Split('/'); //temos que voltar a separar para obter finalmente o porto que se encontra em port[0]
            string[] portFinal = port[0].Split('/');

            Publisher p = new Publisher();

            p.inicialização(args[0],Int32.Parse(portFinal[0]), args[1], args[2], args[3], args[4], args[5]); //[0} = id [1] = urlPublisher, [2]=urlBroker1 [3] = urlBroker2 [4]=urlBroker [5] = urlPuppet

            Console.ReadLine();
        }

        public void inicialização(string id, int port, string url, string urlBroker, string urlBroker2, string urlBroker3, string urlPuppet)
        {
            channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, false);
            objetoPublisher = new publisherObject(urlBroker,url,id,port,urlPuppet, urlBroker2, urlBroker3);
            RemotingServices.Marshal(objetoPublisher, "pub", typeof(publisherObject));

            try
            {
                objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBroker);
                funcaoCallBack = new AsyncCallback(OnExit);
                delRegistoPublisher del = new delRegistoPublisher(objetoBroker.registoPublisher);
                ar = del.BeginInvoke(url, funcaoCallBack, null);

                objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBroker2);
                funcaoCallBack = new AsyncCallback(OnExit);
                delRegistoPublisher del1 = new delRegistoPublisher(objetoBroker.registoPublisher);
                ar = del1.BeginInvoke(url, funcaoCallBack, null);

                objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBroker3);
                funcaoCallBack = new AsyncCallback(OnExit);
                delRegistoPublisher del2 = new delRegistoPublisher(objetoBroker.registoPublisher);
                ar = del2.BeginInvoke(url, funcaoCallBack, null);
            }
            catch (SocketException e) //nao se conseguiu conetar, tenta de novo
            {
                Console.WriteLine("Conexão ao broker falhou, tentando de novo...");
                ChannelServices.UnregisterChannel(channel);
                objetoBroker = null;
                channel = null;
                ar = null;
                funcaoCallBack = null;
                inicialização(id, port, url, urlBroker, urlBroker2, urlBroker3, urlPuppet);
            }
            Console.ReadLine();
        }

        public void OnExit(IAsyncResult ar)
        {
            delRegistoPublisher del = (delRegistoPublisher)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
    }
    public class publisherObject : MarshalByRefObject, IPublisher
    {
        private int seqNumber = 1;

        IBroker objetoBroker = null;
        private string urlBroker;
        private string urlBroker2;
        private string urlBroker3;
        private string id;
        private string url;
        private int port;
        private string urlPuppet;
        bool freezeAtivo = false;

        public bool sentinela = false;
        public bool sentinela2 = false;

        private bool recebeuAtualizacao = false;

        //base para chamadas assíncronas
        public delegate void delRecebeEvento(Evento evento, bool freeze, string urlFilho);
        public delegate void delRecallFunc(int numeroPublicacoes, Evento evento, int intervaloEntrePublicacoes, bool freeze);
        public delegate void delACK(Evento evento, int intervaloEntrePublicacoes);
        public delegate void delSuspeitoDetetado(string urlSuspeito, string url, string urlNovoLider);

        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;

        private List<Evento> listaEventosEsperaACK = new List<Evento>(); // esta lista contém os eventos à espera do ACK do broker que foram enviados com sucesso
        private Dictionary<string, string> listaBrokers = new Dictionary<string, string>();

        static public Object lockSentinela = new Object();

        public publisherObject(string urlBroker, string url,string id, int port, string urlPuppet, string urlBroker2, string urlBroker3)
        {
            this.urlBroker = urlBroker;
            this.url = url;
            this.id = id;
            this.port = port;
            this.urlPuppet = urlPuppet;
            this.urlBroker2 = urlBroker2;
            this.urlBroker3 = urlBroker3;

            listaBrokers.Add(urlBroker, "ATIVO"); //este broker fica predefinido como o primário identificado pelo estado ATIVO , os restantes ficam como backup, no estado ESPERA
            listaBrokers.Add(urlBroker2, "ESPERA");
            listaBrokers.Add(urlBroker3, "ESPERA");
        }

        public void recebeComando(int numeroPublicacoes, Evento evento, int intervaloEntrePublicacoes, bool freeze)
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
                lock(this)
                {
                    if (freezeAtivo)
                        Monitor.Wait(this);
                }

                for (int i = 0; i < numeroPublicacoes; i++)
                {
                    Evento novoEvento = new Evento(evento.getTopic());

                    lock (this)
                    {
                        novoEvento.setContent(id, seqNumber); //atribuimos ao evento o seu conteudo, ou seja o id do publicador do evento e o seqNumber da publicaçao
                        IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                        pm.recebePubLog(id, novoEvento.getTopic(), seqNumber.ToString());
                        novoEvento.setSeqNumberSite(seqNumber);
                        seqNumber++;
                        novoEvento.setUrlOrigem(url);

                    }
                    lock(lockSentinela)
                        listaEventosEsperaACK.Add(novoEvento);


                    delACK del1 = new delACK(verificaEntregaCompleta);
                    funcaoCallBack = new AsyncCallback(OnExit2);
                    ar = del1.BeginInvoke(novoEvento, intervaloEntrePublicacoes, funcaoCallBack, null);                  

                    KeyValuePair<string, string> urlBrokerAtivo = listaBrokers.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo

                    novoEvento.setQuemEnviouEvento("publisher");

                    try
                    {
                        objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);
                        delRecebeEvento del = new delRecebeEvento(objetoBroker.recebeEvento);
                        funcaoCallBack = new AsyncCallback(OnExit5);
                        ar = del.BeginInvoke(novoEvento, false, url, funcaoCallBack, null);
                    }
                    catch (Exception e)
                    {
                        
                    }     

                    Thread.Sleep(intervaloEntrePublicacoes);
                }
            }
        }

        //função é responsável por ver se é necessário reenviar o evento ou nao
        public void verificaEntregaCompleta(Evento evento, int interval)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           

            lock (lockSentinela)
            {
                try
                {
                    if (listaEventosEsperaACK.Any(x => x.getSeqNumber() == evento.getSeqNumber()))
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
                        delRecallFunc del1 = new delRecallFunc(reenviaPub);
                        funcaoCallBack = new AsyncCallback(OnExit3);
                        ar = del1.BeginInvoke(1, evento, interval, false, funcaoCallBack, null);
                    }
                    else
                    {
                    }
                }catch(Exception e) { }
            }
        }

        public void recebeACK(int iden) //ACK enviado pelo broker
        {
           
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }
            lock(lockSentinela)
                listaEventosEsperaACK.RemoveAll(x => x.getSeqNumber() == iden);
        }

        public void reenviaPub(int numeroPublicacoes, Evento evento, int intervaloEntrePublicacoes, bool freeze)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            /*IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
            pm.recebePubLog(id, evento.getTopic(), seqNumber.ToString());*/

            KeyValuePair<string, string> urlBrokerAtivo = listaBrokers.First(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo

            try {
                objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);
                delRecebeEvento del = new delRecebeEvento(objetoBroker.recebeEvento);
                funcaoCallBack = new AsyncCallback(OnExit5);
                ar = del.BeginInvoke(evento, false, url, funcaoCallBack, null);
            }catch(Exception e) { }


            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           

            Thread.Sleep(intervaloEntrePublicacoes);

            lock (lockSentinela)
            {
                if (listaEventosEsperaACK.Any(x => x.getSeqNumber() == evento.getSeqNumber())) //se isto se voltar a verificar, entao houve igualmente um problema com o segundo broker
                {
                    
                    if (!sentinela2)
                    {                        
                        sentinela2 = true;
                        sentinela = false;
                    }
                    delACK del1 = new delACK(verificaEntregaCompleta);
                    funcaoCallBack = new AsyncCallback(OnExit2);
                    ar = del1.BeginInvoke(evento, intervaloEntrePublicacoes, funcaoCallBack, null);
                }
                else
                {
                    if (listaBrokers.Any(x => x.Value.Equals("ESPERA")))
                    {
                        sentinela2 = false;
                        sentinela = false;
                    }
                }
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
            KeyValuePair<string, string> urlBrokerAtivo = listaBrokers.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo

        }

        public void atualizaEstadoBroker(string urlBroker, string estado)
        {
            listaBrokers[urlBroker] = estado;
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

        public int getPort()
        {
             return port;
        }

        public string getUrl()
        {
            return url;
        }

        public string getUrlBroker()
        {
             return urlBroker;
        }



        public void OnExit(IAsyncResult ar)
        {
            delRecebeEvento del = (delRecebeEvento)((AsyncResult)ar).AsyncDelegate;
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

        public void OnExit5(IAsyncResult ar)
        {
            delRecebeEvento del = (delRecebeEvento)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
    }
}

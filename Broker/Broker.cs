using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text;
using System.Threading.Tasks;
using RemoteObjects;
using System.Threading;
using System.Runtime.Remoting.Messaging;

namespace Broker
{
    class Broker
    {
        static private brokerObject objetoBroker;

        //base para chamadas assíncronas
        public delegate void delRegistoBroker(string url);
        public delegate void delBroker(string urlIrmao1, string urlIrmao2, string estado);
        public delegate void delRegisto(string url, string urlPrincipal);
        public delegate void delTabela(string url, int numeroBrokers);
        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;

        static void Main(string[] args)
        {
            Console.WriteLine(args[0] + " criado com sucesso");
            //criar canal
            string[] url = args[1].Split(':'); //separamos para podermos obter o porto para inicializar o canal 
            string[] port = url[2].Split('/'); //temos que voltar a separar para obter finalmente o porto que se encontra em port[0]
            string[] portAux = port[0].Split('/');
            int portFinal = Int32.Parse(portAux[0]);

            int numeroBrokers = Int32.Parse(args[3]);

            TcpChannel channel = new TcpChannel(portFinal);
            ChannelServices.RegisterChannel(channel, false);

            objetoBroker = new brokerObject(args[0], args[1], args[2], numeroBrokers, args[4], args[5],args[6],args[7], args[8]); //[0] = id, [1] = urlBroker [2] url broker pai [4] = urlPuppet [5] = loggingLevel [6,7]= irmaos [8] = site
            RemotingServices.Marshal(objetoBroker, "broker", typeof(brokerObject));

            if(!args[6].Equals("nope")) // se vier alguma coisa no args[6], entao vem a lista dos brokers irmaos que é necessário enviar aos seus irmaos 
            {
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), args[6]); //[8] urlIrmao1
                delBroker del = new delBroker(bro.recebeListaIrmaos);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(args[7],args[1], "ATIVO", funcaoCallBack, null);

                IBroker bro1 = (IBroker)Activator.GetObject(typeof(IBroker), args[7]); //[9] urlIrmao1
                delBroker del1 = new delBroker(bro1.recebeListaIrmaos);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar1 = del1.BeginInvoke(args[6], args[1], "ESPERA", funcaoCallBack, null);


                IBroker bro2 = (IBroker)Activator.GetObject(typeof(IBroker), args[1]); //[9] urlIrmao1
                delRegisto del2 = new delRegisto(bro2.atualizaEstado);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar2 = del2.BeginInvoke("ESPERA", args[7], funcaoCallBack, null);
            }

            //Vamos buscar o pai e adicionamos este broker como seu filho, é o que o if abaixo faz. Para a raiz nao fazemos porque esta nao tem pai
           /* if (!args[2].Equals("raiz"))
            {
                brokerObject objetoBrokerPai = (brokerObject)Activator.GetObject(typeof(brokerObject), args[2]);
                delRegistoBroker del = new delRegistoBroker(objetoBrokerPai.registoBroker);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(args[1], funcaoCallBack, null);
            }*/

            Console.ReadLine();
        }

        static public void OnExit(IAsyncResult ar)
        {
            delRegistoBroker del = (delRegistoBroker)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
    }

    public class brokerObject : MarshalByRefObject, IBroker
    {
        private List<IPublisher> listaPublishers = new List<IPublisher>();
        private List<ISubscriber> listaSubscribers = new List<ISubscriber>();
        private List<BrokerFilho> listaFilhos = new List<BrokerFilho>();
        private Dictionary<string, string> tabelaEncaminhamento = new Dictionary<string, string>();
        private List<Subscricao> tabelaSubscricoes = new List<Subscricao>();
        private List<Evento> listaEventosEspera = new List<Evento>();
        private Dictionary<string, string> listaIrmaos = new Dictionary<string, string>();
        private List<string> listaBrokersSuspeitos = new List<string>();
        private List<Evento> listaEventosEsperaACK = new List<Evento>();
        private List<Evento> listaEventosEsperaACKRegistoSubscriber = new List<Evento>();
        private List<Evento> listaEventosEsperaACKRegistoSubscriber2 = new List<Evento>(); //para unsubscribes
        private List<Evento> listaEventosEsperaACKPropagacaoEvento = new List<Evento>();
        private List<Evento> listaPropagacaoEspera = new List<Evento>();
        private List<Site> listaSites = new List<Site>();
        private List<Pedido> listaPedidoEspera = new List<Pedido>();
        private List<Evento> listaEsperaTotalACK = new List<Evento>();
        private List<Evento> listaEsperaPropagacaoTotalACK = new List<Evento>();

        private string urlBrokerPai;
        private string url;
        private string id;
        private string routingPolicy = "flooding";
        private string orderingPolicy = "FIFO"; //default é FIFO
        private string urlPuppet;
        private string loggingLevel;
        private string site;
        private string brokerPaiSite;

        private string suspeito = "";
        private string auxiliar ="";
        private bool sentinela = false;
        private bool sentinela2 = false;
        private bool sentinela3 = false;
        private bool sentinela4 = false; //utilizado no try do envia filtering

        private string estado; //estado do broker, possiveis: SUSPEITO, ESPERA, ATIVO, MORTO

        private int seqNumberSite = 0;

        private int numeroBrokers;

        private bool podeReceberEventos = false;

        private bool freezeAtivo = false;

        private bool propagou = false; //variavel utilizada para ver se o broker propagou ou nao (caso de flooding). Usada para fins do log.

        private int completo;

        
        
        //Variaveis novas
        private int sequencer = 0;
        private int sequencerRaiz = 0;
        private int numeroPedido = 1;
        private Dictionary<string, int> pedidosSequencer = new Dictionary<string, int>();
        private List<Pedido> filaPedidosEspera = new List<Pedido>();
        private SortedDictionary<int, Evento> eventosEspera_pedido = new SortedDictionary<int, Evento>();
        private List<Pedido> filaPedidosRecebidos = new List<Pedido>();
        private Dictionary<string, int> sequencerSubscritores = new Dictionary<string, int>();
        private Dictionary<string, int> listaPublishersLocais = new Dictionary<string, int>();
        static public Object lock_2 = new Object();
        static public Object lock_3 = new Object();
        static public Object lock_4 = new Object();
        static public Object lock_5 = new Object();
        static public Object lock_6 = new Object();
        static public Object lock_7 = new Object();
       
        private List<Evento> listaEventosEspera2 = new List<Evento>();
        private List<Evento> listaOrganizada = new List<Evento>();
        private List<Site> listaSites2 = new List<Site>();
        

        //base para chamadas assíncronas
        public delegate void delTrata(Evento evento, bool freeze, string urlFilho);
        public delegate void delRegistoSubscriber(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento, string nodeAEnviar, string funcaoAEnviar);
        public delegate void delRegistoSubscriber3(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento, string nodeAEnviar, string funcaoAEnviar, string site);
        public delegate void delRegistoSubscriber2(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento);
        public delegate void delTrataSubscriber(Evento evento, bool freeze);
        public delegate void delPropagacao(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou);
        public delegate void delVerificaPropragacao(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou, string tipoOrdenacao);
        public delegate void delRemove(Evento evento, string quemEnviouEvento);
        public delegate void delACK(int iden);
        public delegate void delACK2(int iden, string site, string nomePublisher);
        public delegate void delTabela(IBroker bro, string url);
        public delegate void delAtualizaLider(string urlNovoLider, string urlSuspeito);
        public delegate void delRegistoBroker(string url, string estado, string site);
        public delegate void delBrokerSuspeito(string url);
        public delegate void delATualizaListaFilhos(List<BrokerFilho> listaFilhos);
        public delegate void delFault(Evento evento, string nodeAEnviar, string site, string urlQuemEnviou);
        public delegate void delSuspeitoDetetado(string urlSuspeito, string urlNovoLider);
        public delegate void delAtualiza(string urlSuspeito, string urlQuemEnviou, string novoLider, string tipoAtualizacao);
        public delegate void delTabelaEncaminhamento(Dictionary<string, string> tabelaEncaminhamento);
        public delegate void delAtualizaSubscribers(Subscricao subs, string urlQuemEnviou);
        public delegate void delEnviaBroker(Evento evento, string urlBrokerPai, string paiOuFilho, string brokerPaiSite, string urlQuemEnviouEvento);
        public delegate void delEnviaFiltering(Evento evento, Subscricao subscricao);
        public delegate void delConfirmacao();
        public delegate void delListaEsperaRegisto(int iden, string idSubscriber);
        public delegate void delTotal(Pedido pedido, string urlQuemEnviou, string nodeAEnviar, string idBrokerDestino);
        public delegate void delPedido(Pedido pedido);
        public delegate void delSequencer(Pedido pedido, string urlQuemEnviou);
        public delegate void delRecebeTotal(Evento evento, string quemEnviouEvento);
        public delegate void delPropagacaoFIFO(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou);
        public delegate void delPedidoACK(int seqNumber, string site, string nomePublisher);

        static public Object lock_ = new Object();
        static public Object _lock_ = new Object();
        static public Object lockEncaminhamento = new Object();
        static public Object lockRegisto = new Object();
        static public Object lockEspera = new Object();
        static public Object lockPropagacao = new Object();
        static public Object lockEventosEspera = new Object();
        static public Object lockRegistoSubscriber = new Object();
        static public Object lockEvento = new Object();
        static public Object lockSentinela = new Object();
        static public Object lockTotal = new Object();


        private bool ajudaLock = false;
        private static bool lockEventos = false; //lock usado para impedir que os eventos sejam enviados até que o broker propague as subscricoes

        public Dictionary<string, string> listaBrokersPais = new Dictionary<string, string>();

        static AsyncCallback funcaoCallBack; //irá chamar uma função quando a função assincrona terminar
        static IAsyncResult ar;

        private string idBrokerPrincipalOriginal;

        public brokerObject(string id, string url, string urlBrokerPai, int numeroBrokers, string urlPuppet, string loggingLevel,string urlIrmao1, string urlIrmao2, string site)
        {
            if (!urlBrokerPai.Equals("raiz"))
            {
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                brokerPaiSite = bro.getSite();
            }
            this.urlBrokerPai = urlBrokerPai;
            this.id = id;
            this.url = url;
            this.numeroBrokers = numeroBrokers;
            this.urlPuppet = urlPuppet;
            this.loggingLevel = loggingLevel;
            this.site = site;

            if (!urlIrmao1.Equals("nope"))
            {
                listaIrmaos.Add(urlIrmao1, "ATIVO");
                listaIrmaos.Add(urlIrmao2, "ESPERA");
            }     
        }

        public void addListaSubscriber(string url)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            ISubscriber subscriber = (ISubscriber)Activator.GetObject(typeof(IPublisher), url);
            listaSubscribers.Add(subscriber);

        }

        public void registoPublisher(string url)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }

            IPublisher publisher = (IPublisher)Activator.GetObject(typeof(IPublisher), url);
            listaPublishers.Add(publisher);
         
        }

        public void recebeListaIrmaos(string urlIrmao1, string urlIrmao2, string estado)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }        
            if (estado.Equals("ESPERA"))
            {
                estado = "ESPERA";
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlIrmao1);
                idBrokerPrincipalOriginal = bro.getID();
                listaIrmaos.Add(urlIrmao1, "ATIVO");
                listaIrmaos.Add(urlIrmao2, "ESPERA");
            }
            else
            {
                listaIrmaos.Add(urlIrmao1, "ESPERA");
                listaIrmaos.Add(urlIrmao2, "ESPERA");             
                estado = "ATIVO";
            }

            if (!urlBrokerPai.Equals("raiz")) // a raiz nao tem pai
            {
                IBroker brokerPai = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                listaBrokersPais = brokerPai.getListaIrmaos();
                listaBrokersPais.Add(urlBrokerPai, "ATIVO");

                // Vamos buscar o pai e adicionamos este broker como seu filho, é o que o if abaixo faz. Para a raiz nao fazemos porque esta nao tem pai
                try
                {
                    
                    IBroker objetoBrokerPai = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                    /*delRegistoBroker del = new delRegistoBroker(objetoBrokerPai.registoBroker);
                    funcaoCallBack = new AsyncCallback(OnExit7);
                    IAsyncResult ar =del.BeginInvoke(url, estado, site, funcaoCallBack, null);*/
                    objetoBrokerPai.registoBroker(url, estado, site);
                }
                catch(Exception e) { }        
            }

        }

        public void atualizaEstado(string state, string urlPrincipal)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }
            IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlPrincipal);
            idBrokerPrincipalOriginal = bro.getID();

            estado = state;

            if (!urlBrokerPai.Equals("raiz")) // a raiz nao tem pai
            {
                try
                {
                    IBroker brokerPai = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                    listaBrokersPais = brokerPai.getListaIrmaos();
                    listaBrokersPais.Add(urlBrokerPai, "ATIVO");

                    // Vamos buscar o pai e adicionamos este broker como seu filho, é o que o if abaixo faz. Para a raiz nao fazemos porque esta nao tem pai

                    IBroker objetoBrokerPai = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                    /*delRegistoBroker del = new delRegistoBroker(objetoBrokerPai.registoBroker);
                    funcaoCallBack = new AsyncCallback(OnExit7);
                    IAsyncResult ar =del.BeginInvoke(url, estado, site, funcaoCallBack, null);*/
                    objetoBrokerPai.registoBroker(url, estado, site);
                }
                catch(Exception e) { }
            }
        }

        public void recebeCrash()
        {
            System.Environment.Exit(1);
        }

        //fucao responsável por ver se o broker realmente crashou ou apenas está freeze(lento)
        //também responsável por avisar que há um novo broker Principal
        public void detetaSuspeito(string urlBrokerSuspeito, string urlQuemEnviou, string novoLider)
        {
            if (suspeito.Equals(urlBrokerSuspeito)) //só analisamos um suspeito de cada vez(de cada tipo)
                return;

            suspeito = urlBrokerSuspeito;
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento                   
                        return;                    
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string,string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar1 =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            lock(this)
                listaIrmaos[urlBrokerSuspeito] = "SUSPEITO";
            estado = "ATIVO";

            lock(lock_)
                ajudaLock = true;

            //Atualizamos os estados nos brokers/subscribers/publishers

            delAtualiza del20 = new delAtualiza(envioAtualizacoes);
            funcaoCallBack = new AsyncCallback(OnExit44);
            IAsyncResult ar =del20.BeginInvoke(novoLider, urlBrokerSuspeito, urlQuemEnviou, "lider", funcaoCallBack, null);

            //vamos confirmar se o broker crashou ou nao enviando uma mensagem e vendo se obtemos resposta, esta mensagem terá um delay maior até confirmar se o broker realmente crashou ou nao
            IBroker brokerSuspeito = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerSuspeito);
            listaBrokersSuspeitos.Add(urlBrokerSuspeito);

            try
            {
                delBrokerSuspeito del2 = new delBrokerSuspeito(brokerSuspeito.ping);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar1 = del2.BeginInvoke(url, funcaoCallBack, null);
            }catch(Exception e) { }

            Thread.Sleep(20000);
            lock(this)
            {
                if (listaBrokersSuspeitos.Any(x => x.Equals(urlBrokerSuspeito))) //se isto se voltar a verificar, entao o broker é dado como morto
                {
                    listaIrmaos[urlBrokerSuspeito] = "MORTO";

                    delAtualiza del5 = new delAtualiza(envioAtualizacoes);
                    funcaoCallBack = new AsyncCallback(OnExit44);
                    IAsyncResult ar1 =del5.BeginInvoke(novoLider, urlBrokerSuspeito, urlQuemEnviou, "crash", funcaoCallBack, null);

                }
                else //caso contrário retiramo-lo do estado suspeito e metemo-lo no estado espera
                {
                    listaIrmaos[urlBrokerSuspeito] = "ESPERA";

                    delAtualiza del5 = new delAtualiza(envioAtualizacoes);
                    funcaoCallBack = new AsyncCallback(OnExit44);
                    IAsyncResult ar1 = del5.BeginInvoke(novoLider, urlBrokerSuspeito, urlQuemEnviou, "espera", funcaoCallBack, null);
                }
            }
        }

        public void envioAtualizacoes(string urlNovoLider, string urlBrokerSuspeito, string urlQuemEnviou, string tipoAtualizacao)
        {

            foreach (IPublisher pub in listaPublishers)
            {
                if (pub.getUrl().Equals(urlQuemEnviou)) //quem enviou ja sabe quem é o novo lider
                    continue;
                if (tipoAtualizacao.Equals("lider"))
                {
                    delAtualizaLider del = new delAtualizaLider(pub.atualizaLider);
                    funcaoCallBack = new AsyncCallback(OnExit5);
                    IAsyncResult ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, funcaoCallBack, null);
                }
                else if(tipoAtualizacao.Equals("espera"))
                {
                    delAtualizaLider del = new delAtualizaLider(pub.atualizaEstadoBroker);
                    funcaoCallBack = new AsyncCallback(OnExit5);
                    IAsyncResult ar =del.BeginInvoke(urlBrokerSuspeito, "ESPERA", funcaoCallBack, null);
                }
                else if(tipoAtualizacao.Equals("crash"))
                {
                    delAtualizaLider del = new delAtualizaLider(pub.atualizaEstadoBroker);
                    funcaoCallBack = new AsyncCallback(OnExit5);
                    IAsyncResult ar =del.BeginInvoke(urlBrokerSuspeito, "MORTO", funcaoCallBack, null);
                }
            }

            foreach (ISubscriber sub in listaSubscribers)
            {
                if (sub.getUrl().Equals(urlQuemEnviou)) //quem enviou ja sabe quem é o novo lider
                    continue;

                if (tipoAtualizacao.Equals("lider"))
                {
                    delAtualizaLider del = new delAtualizaLider(sub.atualizaLider);
                    funcaoCallBack = new AsyncCallback(OnExit5);
                    IAsyncResult ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, funcaoCallBack, null);
                }
                else if (tipoAtualizacao.Equals("espera"))
                {
                    delAtualizaLider del = new delAtualizaLider(sub.atualizaEstadoBroker);
                    funcaoCallBack = new AsyncCallback(OnExit5);
                    IAsyncResult ar =del.BeginInvoke(urlBrokerSuspeito, "ESPERA", funcaoCallBack, null);
                }
                else if (tipoAtualizacao.Equals("crash"))
                {
                    delAtualizaLider del = new delAtualizaLider(sub.atualizaEstadoBroker);
                    funcaoCallBack = new AsyncCallback(OnExit5);
                    IAsyncResult ar =del.BeginInvoke(urlBrokerSuspeito, "MORTO", funcaoCallBack, null);
                }
            }
            //temos tambem que atualizar os brokers pais
            try
            {
                foreach (KeyValuePair<string, string> bro in listaBrokersPais)
                {
                    IBroker broPai = (IBroker)Activator.GetObject(typeof(IBroker), bro.Key);
                    if (!urlQuemEnviou.Equals(broPai.getUrl()))// nao atualizamos quem enviou a detetacao pois ja esta atualizado
                    {
                        if (routingPolicy.Equals("filtering")) //se a routing policy for filtering, entao temos que atualizar a tabela de encaminhamento do broker
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broPai.atualizaTabelaEncaminhamento);
                                funcaoCallBack = new AsyncCallback(OnExit7);
                                IAsyncResult ar =del.BeginInvoke(id, urlNovoLider, urlBrokerSuspeito, funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        if (tipoAtualizacao.Equals("espera"))
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broPai.atualizaEstadoBrokerPaiVindoFilho);
                                funcaoCallBack = new AsyncCallback(OnExit7);
                                IAsyncResult ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, "ESPERA", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        else if (tipoAtualizacao.Equals("lider"))
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broPai.atualizaEstadoBrokerPaiVindoFilho);
                                funcaoCallBack = new AsyncCallback(OnExit7);
                                IAsyncResult ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, "SUSPEITO", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        else if (tipoAtualizacao.Equals("crash"))
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broPai.atualizaEstadoBrokerPaiVindoFilho);
                                funcaoCallBack = new AsyncCallback(OnExit7);
                                IAsyncResult ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, "MORTO", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                    }
                }
            }
            catch (Exception e) { }

            //atualizamos os filhos
            try
            {
                foreach (BrokerFilho bro in listaFilhos)
                {
                    IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), bro.getBroker());

                    if (!urlQuemEnviou.Equals(broFilho.getUrl()))// nao atualizamos quem enviou a detetacao pois ja esta atualizado
                    {
                        if (routingPolicy.Equals("filtering")) //se a routing policy for filtering, entao temos que atualizar a tabela de encaminhamento do broker
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broFilho.atualizaTabelaEncaminhamento);
                                funcaoCallBack = new AsyncCallback(OnExit6);
                                IAsyncResult ar =del.BeginInvoke(id, urlNovoLider, urlBrokerSuspeito, funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        if (tipoAtualizacao.Equals("espera"))
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broFilho.atualizaEstadoBrokerPai);
                                funcaoCallBack = new AsyncCallback(OnExit6);
                                IAsyncResult  ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, "ESPERA", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        else if (tipoAtualizacao.Equals("lider"))
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broFilho.atualizaEstadoBrokerPai);
                                funcaoCallBack = new AsyncCallback(OnExit6);
                                IAsyncResult ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, "SUSPEITO", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        else if (tipoAtualizacao.Equals("crash"))
                        {
                            try
                            {
                                delRegistoBroker del = new delRegistoBroker(broFilho.atualizaEstadoBrokerPai);
                                funcaoCallBack = new AsyncCallback(OnExit6);
                                IAsyncResult ar =del.BeginInvoke(urlNovoLider, urlBrokerSuspeito, "MORTO", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                    }
                }
            }
            catch(Exception e) { }
            //avisamos os irmaos
            lock (this)
            {
                try
                {
                    foreach (KeyValuePair<string, string> brokIrmao in listaIrmaos)
                    {
                        if (tipoAtualizacao.Equals("espera"))
                        {
                            try
                            {
                                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), brokIrmao.Key);

                                delAtualizaLider del = new delAtualizaLider(bro.atualizaEstadoBroker);
                                funcaoCallBack = new AsyncCallback(OnExit5);
                                IAsyncResult ar =del.BeginInvoke(urlBrokerSuspeito, "ESPERA", funcaoCallBack, null);
                            }
                            catch (Exception e) { }

                            try
                            {
                                IBroker bro1 = (IBroker)Activator.GetObject(typeof(IBroker), brokIrmao.Key);
                                delAtualizaLider del = new delAtualizaLider(bro1.atualizaEstadoBroker);
                                funcaoCallBack = new AsyncCallback(OnExit5);
                                IAsyncResult ar =del.BeginInvoke(urlNovoLider, "ATIVO", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                        else if (tipoAtualizacao.Equals("crash"))
                        {
                            try
                            {

                                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), brokIrmao.Key);

                                delAtualizaLider del = new delAtualizaLider(bro.atualizaEstadoBroker);
                                funcaoCallBack = new AsyncCallback(OnExit5);
                                IAsyncResult ar =del.BeginInvoke(urlBrokerSuspeito, "MORTO", funcaoCallBack, null);
                            }
                            catch (Exception e) { }

                            try
                            {
                                IBroker bro1 = (IBroker)Activator.GetObject(typeof(IBroker), brokIrmao.Key);

                                delAtualizaLider del = new delAtualizaLider(bro1.atualizaEstadoBroker);
                                funcaoCallBack = new AsyncCallback(OnExit5);
                                IAsyncResult ar =del.BeginInvoke(urlNovoLider, "ATIVO", funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                    }
                }
                catch(Exception e) { }
            }       
        }

        //funcao invocado quando um broker é declarado morto quando afinal nao estava
        public void estouVivo(string urlQuemEnviou)
        {
           
            if(!auxiliar.Equals(urlQuemEnviou)) //só trata um pedido de estouVivo vindo de um determinado broker
            {
                auxiliar =urlQuemEnviou;
                listaIrmaos[urlQuemEnviou] = "ESPERA"; //trocamos o seu estado de MORTO para ESPERA

                delAtualiza del5 = new delAtualiza(envioAtualizacoes);
                funcaoCallBack = new AsyncCallback(OnExit4);
                IAsyncResult ar =del5.BeginInvoke(url, urlQuemEnviou, urlQuemEnviou, "espera", funcaoCallBack, null);
            }

        }

        //função invocada quando um broker deteta que um seu filho ou pai está suspeito
        //esta função é responsável alertar o respetivo site que o seu broker principal está suspeito
        public void brokerDetetaSuspeito(string urlBrokerSuspeito, string urlNovoLider, string urlQuemEnviou)
        {
            try
            {
                IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoLider);
                /*   delRegistoBroker del2 = new delRegistoBroker(broker.detetaSuspeito);
                   funcaoCallBack = new AsyncCallback(OnExit7);
                   IAsyncResult ar =del2.BeginInvoke(urlBrokerSuspeito, urlQuemEnviou, urlNovoLider, funcaoCallBack, null);*/
                broker.detetaSuspeito(urlBrokerSuspeito, urlQuemEnviou, urlNovoLider);
            }catch(Exception e)
            { }
        }
 
        public void ping(string urlQuemEnviouPing)
        {
            estado = "SUSPEITO";
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }
            try
            {
                IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouPing);

                delBrokerSuspeito del2 = new delBrokerSuspeito(broker.recebeAck);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del2.BeginInvoke(url, funcaoCallBack, null);
            }catch(Exception e) { }
        }

        public void atualizaEstadoBrokerPai(string urlNovoPai, string urlBrokerSuspeito, string estado)
        {
            lock (this)
            {
                if (freezeAtivo)
                    Monitor.Wait(this);
            }
            lock(lockSentinela)
            {
                sentinela3 = true;
                sentinela = true;
            }

            urlBrokerPai = urlNovoPai;
            listaBrokersPais[urlBrokerSuspeito] = estado;
            if(listaBrokersPais.Any(x => x.Value.Equals("ATIVO")))
            {
                KeyValuePair<string, string> brokerMalAtivo = listaBrokersPais.First(x => x.Value.Equals("ATIVO"));
                listaBrokersPais[brokerMalAtivo.Key] = estado;
            }
            listaBrokersPais[urlNovoPai] = "ATIVO";
           
            foreach (KeyValuePair<string, string> brokIrmao in listaIrmaos)
            {
                try
                {
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), brokIrmao.Key);

                    delRegistoBroker del = new delRegistoBroker(bro.irmaoAtualizaBrokerPai);
                    funcaoCallBack = new AsyncCallback(OnExit7);
                    IAsyncResult ar =del.BeginInvoke(urlNovoPai, urlBrokerSuspeito, estado, funcaoCallBack, null);
                }
                catch (Exception e) { }              
            }
        }

        public void atualizaEstadoBrokerPaiVindoFilho(string urlNovoFilho, string urlBrokerSuspeito, string estado)
        {
            lock (lockSentinela)
            {
                sentinela3 = true;
                sentinela = true;
            }
            lock (this)
            {
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoFilho);

                BrokerFilho filhoAtualizar =listaFilhos.Find(x => x.getSite().Equals(bro.getSite()));
                filhoAtualizar.setEstado(urlBrokerSuspeito, estado);
                if (filhoAtualizar.existeAtivo())
                {
                    string urlMalAtivo = filhoAtualizar.getAtivo();
                    filhoAtualizar.setEstado(urlMalAtivo, estado);
                }
                filhoAtualizar.setEstado(urlNovoFilho, "ATIVO");
            }
        }

        public void atualizaEstadoBroker(string urlBroker, string estado)
        {

            if (urlBroker.Equals(url))
                this.estado = estado;
            else
                lock(this)
                 listaIrmaos[urlBroker] = estado;
        }

        public void irmaoAtualizaBrokerPai(string novoUrl, string urlBrokerSuspeito, string estado)
        {
            lock(lockSentinela)
            {
                sentinela3 = true;
                sentinela = true;
            }

            urlBrokerPai = novoUrl;
            listaBrokersPais[urlBrokerSuspeito] = estado;
            
            if (listaBrokersPais.Any(x => x.Value.Equals("ATIVO")))
            {
                KeyValuePair<string, string> brokerMalAtivo = listaBrokersPais.First(x => x.Value.Equals("ATIVO"));
                listaBrokersPais[brokerMalAtivo.Key] = estado;
            }
            listaBrokersPais[novoUrl] = "ATIVO";
            
            
        }

        public void recebeAck(string urlSuspeito)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            listaBrokersSuspeitos.Remove(urlSuspeito);
        }

        public void recebeAckBrokers(int iden, string site, string nomePublisher)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lockEspera)
                listaEventosEsperaACK.RemoveAll(x => x.getSeqNumber() == iden && x.getSite().Equals(site) && x.getNomePublisher().Equals(nomePublisher));
        }

        public void recebeAckPropagacao(int iden, string site, string nomePublisher)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            lock(lockPropagacao)
                listaEventosEsperaACKPropagacaoEvento.RemoveAll(x => x.getSeqNumber() == iden && x.getSite().Equals(site) && x.getNomePublisher().Equals(nomePublisher));

        }

        public void registoSubscriber(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            string aux = "";
            if (evento.getQuemEnviouEvento().Equals("subscriber")) //envio do ACK
            {
                aux = "subscriber";

            }
            if (routingPolicy.Equals("filtering"))
            {
                lock (lockEncaminhamento)
                {
                    if (!podeReceberEventos) //se esta condiçao der falso, quer dizer que as tabelas de encaminhamento ainda estao a ser processadas e os eventos ainda nao podem ser processados (menos no caso do flooding)
                    {
                        Monitor.Wait(lockEncaminhamento);
                    }
                }
            }

            Subscricao subscricao = new Subscricao(id, evento, url);
            tabelaSubscricoes.Add(subscricao);
            List<Subscricao> tabelaAux = new List<Subscricao>();

            //remover duplicados
            for (int i = 0; i< tabelaSubscricoes.Count; i++)
            {
                if (tabelaAux.Count == 0)
                    tabelaAux.Add(tabelaSubscricoes[i]);

                if(!tabelaAux.Any(x => x.getEvento().getTopic().Equals(tabelaSubscricoes[i].getEvento().getTopic()) && x.getIDSubscriber().Equals(tabelaSubscricoes[i].getIDSubscriber())))
                {
                    tabelaAux.Add(tabelaSubscricoes[i]);
                }
            }
            tabelaSubscricoes = tabelaAux;



            /* FILTER
                Para o filter o nosso algoritmo é o seguinte:
                Em primeiro lugar criamos as tabelas de encaminhamento para todos os brokers
                Em segundo lugar propagamos as subscricoes para todos os brokers de maneira a que estes
                saibam para que brokers têm que reencaminhar o evento para chegar ao subscriber desejado
                sem ter que enviar para brokers desnecessários
            */

            if (routingPolicy.Equals("filtering"))
            {
                evento.setQuemEnviouEvento("broker");

                if (!listaIrmaos.ContainsKey(urlQuemEnviouEvento)) //se vier de um irmao, entao nao propagamos, apenas o principal propaga
                {
                    if (!urlBrokerPai.Equals("raiz") && !urlQuemEnviouEvento.Equals(urlBrokerPai)) // se nao for a raiz, propagamos pelo pai, se o URL vier do Pai, também nao propagamos de volta para ele
                    {
                        lock(lockRegistoSubscriber);
                            listaEventosEsperaACKRegistoSubscriber.Add(evento);

                        delRegistoSubscriber3 del2 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                        funcaoCallBack = new AsyncCallback(OnExit10);
                        IAsyncResult ar =del2.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, "pai", "subscribe", "", funcaoCallBack, null);

                        try
                        {
                            IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                            lockEventos = true;
                            delRegistoSubscriber2 del = new delRegistoSubscriber2(bro.registoSubscriber);
                            funcaoCallBack = new AsyncCallback(OnExit8);
                            IAsyncResult ar1 =del.BeginInvoke(id, url, comando, evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                        }
                        catch(Exception e) {
                        }
                    }

                    foreach (BrokerFilho bro in listaFilhos) //propaga a subscricao para os filhos
                    {
                        string site = bro.getSite();
                        Evento novoEvento = new Evento(evento.getTopic());
                        int iden = evento.getIdentificador();
                        iden++;
                        novoEvento.setIdSubscriber(evento.getIdSubscriber());
                        novoEvento.setIdentificador(iden);
                        novoEvento.setComando(comando);
                        novoEvento.setQuemEnviouEvento("broker");
                        try
                        {
                            IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), bro.getBroker());


                            if (!urlQuemEnviouEvento.Equals(broFilho.getUrl())) //no caso em que propagamos para o filho, nao propagamos para o filho que enviou para o pai
                            {

                                lock(lockRegistoSubscriber);
                                    listaEventosEsperaACKRegistoSubscriber.Add(evento);

                                delRegistoSubscriber3 del2 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                                funcaoCallBack = new AsyncCallback(OnExit10);
                                IAsyncResult ar =del2.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, "filho", "subscribe", site, funcaoCallBack, null);

                                lockEventos = true;

                                delRegistoSubscriber2 del = new delRegistoSubscriber2(broFilho.registoSubscriber);
                                funcaoCallBack = new AsyncCallback(OnExit8);
                                IAsyncResult ar1 =del.BeginInvoke(id, url, comando, evento, this.url, funcaoCallBack, null);
                            }
                        }
                        catch (Exception e) {
                            lock(lockRegistoSubscriber);
                                listaEventosEsperaACKRegistoSubscriber.Add(evento);

                            delRegistoSubscriber3 del2 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                            funcaoCallBack = new AsyncCallback(OnExit10);
                            IAsyncResult ar =del2.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, "filho", "subscribe", site, funcaoCallBack, null);
                        }
                    }
                }               
            }

            if (!listaIrmaos.ContainsKey(urlQuemEnviouEvento)) //se vier de um irmao, entao nao propagamos de volta pois este ja o tem
                foreach (KeyValuePair<string, string> urlBrokIrmao in listaIrmaos) //vamos enviar a subscricao para os irmaos
                {
                    try
                    {
                        IBroker brokIrmao = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokIrmao.Key);
                        delAtualizaSubscribers del = new delAtualizaSubscribers(brokIrmao.atualizaListaSubscriber);
                        funcaoCallBack = new AsyncCallback(OnExit8);
                        IAsyncResult ar =del.BeginInvoke(subscricao,this.url, funcaoCallBack, null);
                        // brokIrmao.atualizaListaSubscriber(subscricao, this.url);
                    }
                    catch (Exception e) {
                        if (aux.Equals("subscriber"))
                        {
                            try
                            {
                                ISubscriber subs = (ISubscriber)Activator.GetObject(typeof(ISubscriber), url);
                                delACK del = new delACK(subs.recebeACK);
                                funcaoCallBack = new AsyncCallback(OnExit);
                                IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(), funcaoCallBack, null);
                            }
                            catch (Exception d) { }
                        }
                        if (evento.getQuemEnviouEvento().Equals("broker"))
                        {
                            try
                            {
                                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                                delListaEsperaRegisto del = new delListaEsperaRegisto(bro.recebeACKRegistoSubscriber);
                                funcaoCallBack = new AsyncCallback(OnExit);
                                IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(), evento.getIdSubscriber(), funcaoCallBack, null);
                            }
                            catch (Exception f) { }
                        }
                    }
                }

            if (aux.Equals("subscriber"))
            {      
                try
                {
                    ISubscriber subs = (ISubscriber)Activator.GetObject(typeof(ISubscriber), url);
                    delACK del = new delACK(subs.recebeACK);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(), funcaoCallBack, null);
                }
                catch (Exception e) { }
            }
            if (evento.getQuemEnviouEvento().Equals("broker"))
            {
                try
                {
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                    delListaEsperaRegisto del = new delListaEsperaRegisto(bro.recebeACKRegistoSubscriber);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(), evento.getIdSubscriber(), funcaoCallBack, null);
                }
                catch (Exception e) { }
            }
        }

        public void unsubscribe(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            if (routingPolicy.Equals("filtering"))
            {
                lock (lockEncaminhamento)
                {
                    if (!podeReceberEventos) //se esta condiçao der falso, quer dizer que as tabelas de encaminhamento ainda estao a ser processadas e os eventos ainda nao podem ser processados (menos no caso do flooding)
                    {

                        Monitor.Wait(lockEncaminhamento);
                    }
                }
            }
            tabelaSubscricoes.Remove(tabelaSubscricoes.Find(x => x.getEvento().getTopic().Equals(evento.getTopic()) && x.getIDSubscriber().Equals(evento.getIdSubscriber())));

            string aux = "";
            if (evento.getQuemEnviouEvento().Equals("subscriber")) //envio do ACK
            {
                aux = "subscriber";
            }

            //se for filtering, temos que remover a subscricao de todos os brokers
            if (routingPolicy.Equals("filtering"))
            {
                evento.setQuemEnviouEvento("broker");

                if (!listaIrmaos.ContainsKey(urlQuemEnviouEvento)) // se vier de um irmao então nao propagamos, só o principal propaga
                {
                    if (!urlBrokerPai.Equals("raiz") && !urlQuemEnviouEvento.Equals(urlBrokerPai)) // se nao for a raiz, propagamos pelo pai, se o URL vier do Pai, também nao propagamos de volta para ele
                    {
                        lock(lockRegistoSubscriber)
                            listaEventosEsperaACKRegistoSubscriber2.Add(evento);

                        delRegistoSubscriber3 del2 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                        funcaoCallBack = new AsyncCallback(OnExit10);
                        IAsyncResult ar =del2.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, "pai", "unsubscribe", "", funcaoCallBack, null);

                        try
                        {
                            IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                            lockEventos = true;
                            delRegistoSubscriber2 del = new delRegistoSubscriber2(bro.unsubscribe);
                            funcaoCallBack = new AsyncCallback(OnExit8);
                            IAsyncResult ar1 =del.BeginInvoke(id, url, comando, evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                        }
                        catch (Exception e) {
                            lock (lockRegistoSubscriber)
                                   listaEventosEsperaACKRegistoSubscriber2.Add(evento);

                            delRegistoSubscriber3 del1 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                            funcaoCallBack = new AsyncCallback(OnExit10);
                            IAsyncResult ar1 =del1.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, "pai", "unsubscribe", site, funcaoCallBack, null);
                        }
                    }
                    foreach (BrokerFilho bro in listaFilhos) //propaga a subscricao para os filhos
                    {
                        string site = bro.getSite();
                        Evento novoEvento = new Evento(evento.getTopic());
                        int iden = evento.getIdentificador();
                        iden++;
                        novoEvento.setIdSubscriber(evento.getIdSubscriber());
                        novoEvento.setIdentificador(iden);
                        novoEvento.setComando(comando);
                        novoEvento.setQuemEnviouEvento("broker");
                        try
                        { 
                            IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), bro.getBroker());

                            if (!urlQuemEnviouEvento.Equals(broFilho.getUrl())) //no caso em que propagamos para o filho, nao propagamos para o filho que enviou para o pai
                            {
                                
                                lock(lockRegistoSubscriber)
                                    listaEventosEsperaACKRegistoSubscriber2.Add(novoEvento);

                                delRegistoSubscriber3 del2 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                                funcaoCallBack = new AsyncCallback(OnExit10);
                                IAsyncResult ar =del2.BeginInvoke(id, url, comando, novoEvento, urlQuemEnviouEvento, "filho", "unsubscribe", site, funcaoCallBack, null);

                                lockEventos = true;

                                delRegistoSubscriber2 del = new delRegistoSubscriber2(broFilho.unsubscribe);
                                funcaoCallBack = new AsyncCallback(OnExit8);
                                IAsyncResult ar1 =del.BeginInvoke(id, url, comando, novoEvento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                            }
                        }catch(Exception e) // se entretanto houver um crash
                        {
                            lock (lockRegistoSubscriber)
                                listaEventosEsperaACKRegistoSubscriber2.Add(novoEvento);

                            delRegistoSubscriber3 del2 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                            funcaoCallBack = new AsyncCallback(OnExit10);
                            IAsyncResult ar =del2.BeginInvoke(id, url, comando, novoEvento, urlQuemEnviouEvento, "filho", "unsubscribe", site, funcaoCallBack, null);
                        }
                    }
                }
            }

            if (!listaIrmaos.ContainsKey(urlQuemEnviouEvento)) //se vier de um irmao, entao nao propagamos de volta pois este ja o tem
            {
                foreach (KeyValuePair<string, string> urlBrokIrmao in listaIrmaos) //vamos avisar os irmaos que houve um unsubscribe
                {
                    try
                    {
                        IBroker brokIrmao = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokIrmao.Key);
                        delRemove del = new delRemove(brokIrmao.atualizaListaSubscriber2);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit17);
                        IAsyncResult ar =del.BeginInvoke(evento, this.url, funcaoCallBack, null);
                    }catch(Exception e) {
                        if (aux.Equals("subscriber"))
                        {
                            try
                            {
                                ISubscriber subs = (ISubscriber)Activator.GetObject(typeof(ISubscriber), url);
                                delACK del = new delACK(subs.recebeACK);
                                funcaoCallBack = new AsyncCallback(OnExit);
                                IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(), funcaoCallBack, null);
                            }
                            catch (Exception d) { }
                        }
                        if (evento.getQuemEnviouEvento().Equals("broker"))
                        {
                            try
                            {
                                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                                delListaEsperaRegisto del = new delListaEsperaRegisto(bro.recebeACKUnsubscribe);
                                funcaoCallBack = new AsyncCallback(OnExit);
                                IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(), evento.getIdSubscriber(), funcaoCallBack, null);
                            }
                            catch (Exception f) { }
                        }
                    }
                }
            }
      

            if (aux.Equals("subscriber")) //envio do ACK
            {
                ISubscriber subs = (ISubscriber)Activator.GetObject(typeof(ISubscriber), url);
                delACK del = new delACK(subs.recebeACK2);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(), funcaoCallBack, null);
            }

            if (evento.getQuemEnviouEvento().Equals("broker"))
            {
                try
                {
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                    delListaEsperaRegisto del = new delListaEsperaRegisto(bro.recebeACKUnsubscribe);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(evento.getIdentificador(),evento.getIdSubscriber(), funcaoCallBack, null);
                }
                catch (Exception e) { }
            }
        }

        public void recebeStatus()
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            Console.WriteLine(id + " está presente ");

            //Subscricoes ativas
            Console.WriteLine("Subscrições ativas ");

            foreach (Subscricao entrada1 in tabelaSubscricoes)
            {               
                    Console.WriteLine("Subscritor : " + entrada1.getIDSubscriber() + " Subscriscao : " + entrada1.getEvento().getTopic());
            }
        }

        public void recebeEvento(Evento evento, bool freeze, string urlQuemEnviouEvento)
        {
            if (evento == null) //se no evento vier nada, entao é um comando de freeze/unfreeze
            {                
                lock (this)
                {
                    if (!freeze) //se vier a false, entao vem o unfreeze
                    {
                        freezeAtivo = false;
                        Monitor.PulseAll(this);
                    }
                    else if (freeze)
                        freezeAtivo = true;
                }
            }
            else //se for um comando que nao seja freeze
            {
                lock (this)
                {
                    if (freezeAtivo)
                    {
                        Monitor.Wait(this);
                        if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                            return;
                        else if(estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                        {
                            KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                            IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                            delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                            funcaoCallBack = new AsyncCallback(OnExit4);
                            IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                            return;
                        }
                       
                    }
                }
                /*
                -- FLOODING--
                O que faz é o seguinte:
                Propaga a mensagem pelo broker pai e os seus brokers filhos. Quando propagamos para o broker Pai,
                o pai faz uma verificação que é para nao propagar a mensagem pelo broker(seu filho) que
                lhe enviou a mensagem, porque estaria a repetir a propagação, tal como quando é feita uma propagação
                através do Pai fazemos uma verificação para nao enviar de novo para ele.
                Propagação feita apenas até aos brokers.
                 */
                if (routingPolicy.Equals("flooding")) //default é flooding
                {
                    
                    lock (lock_)
                    {
                        if (ajudaLock)
                            Monitor.Wait(lock_);
                    }

                    if (orderingPolicy.Equals("TOTAL"))     //Total order
                    {

                        lock (lockEncaminhamento)
                        {
                            if (!podeReceberEventos) //se esta condiçao der falso, quer dizer que as tabelas de encaminhamento ainda estao a ser processadas e os eventos ainda nao podem ser processados (menos no caso do flooding)
                            {

                                Monitor.Wait(lockEncaminhamento);
                            }
                        }

                        lock (lock_3)
                        {
                            if (!listaPublishersLocais.ContainsKey(evento.getNomePublisher()))
                                listaPublishersLocais.Add(evento.getNomePublisher(), 0);

                            if (evento.getSeqNumberSite() == listaPublishersLocais[evento.getNomePublisher()] + 1)
                            {

                                Pedido pedido = new Pedido(getUrl(), 0, numeroPedido, getID());   //Cria o pedido

                                if (!urlBrokerPai.Equals("raiz"))        //Se este broker não for a raiz, temos de enviar um pedido à raiz para saber o sequencer
                                {
                                    lock(lock_3)
                                        listaPedidoEspera.Add(pedido);

                                    delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                    IAsyncResult ar1 =del3.BeginInvoke(pedido,this.url, "raiz","", funcaoCallBack, null);

                                    try
                                    {
                                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                                        delSequencer del = new delSequencer(bro.pedidoSequencer);
                                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                        IAsyncResult ar2 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                    }catch(Exception e) { }
                                }
                                else   //Caso contrario, este broker é a raiz (i.e. o publisher encontra-se aqui), logo perguntamos localmente o sequencer
                                {
                                    delSequencer del = new delSequencer(pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar1 =del.BeginInvoke(pedido, urlQuemEnviouEvento, funcaoCallBack, null);
                                }

                                eventosEspera_pedido.Add(numeroPedido, evento);     //Adiciona o evento a um dictionary com o numero de pedido associado

                                numeroPedido++;
                                listaPublishersLocais[evento.getNomePublisher()]++;

                                delConfirmacao del1 = new delConfirmacao(verificaFilaEsperaTotal);
                                funcaoCallBack = new AsyncCallback(OnExit14);
                                IAsyncResult ar =del1.BeginInvoke(funcaoCallBack, null);


                            }
                            else
                                listaEventosEspera.Add(evento);
                        }
                    }
                    else //caso contrario é NO order ou FIFO
                    {
                        if (!urlBrokerPai.Equals("raiz") && !urlQuemEnviouEvento.Equals(urlBrokerPai)) // se nao for a raiz, propagamos pelo pai, se o URL vier do Pai, também nao propagamos de volta para ele
                        {
                            propagou = true;
                            delEnviaBroker del = new delEnviaBroker(enviaBroker);
                            funcaoCallBack = new AsyncCallback(OnExit12);
                            IAsyncResult ar =del.BeginInvoke(evento, urlBrokerPai, "pai", brokerPaiSite, urlQuemEnviouEvento, funcaoCallBack, null);

                            //enviaBroker(evento, urlBrokerPai, "pai", brokerPaiSite, urlQuemEnviouEvento);      
                        }
                        foreach (BrokerFilho bro in listaFilhos) //propaga o evento para os filhos
                        {
                            try
                            {
                                if (!urlQuemEnviouEvento.Equals(bro.getBroker())) //no caso em que propagamos para o filho, nao propagamos para o filho que enviou para o pai
                                {
                                    propagou = true;
                                    delEnviaBroker del = new delEnviaBroker(enviaBroker);
                                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit12);
                                    IAsyncResult ar =del.BeginInvoke(evento, bro.getBroker(), "filho", bro.getSite(), urlQuemEnviouEvento, funcaoCallBack, null);

                                    //enviaBroker(evento, bro.getBroker(), "filho", bro.getSite(),urlQuemEnviouEvento);                           
                                }
                            }
                            catch (Exception e) //foi detetado um crash
                            {
                            }

                        }

                        //se nao propagou é o ultimo broker que passou o o evento do flooding e necessitamos de registar que passou tambem por este broker
                        if (!propagou)
                            if (loggingLevel.Equals("full"))
                            {
                                IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                                pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), "");
                            }

                        if (orderingPolicy.Equals("FIFO"))
                        {

                            lock(lockEventosEspera)
                            {
                                if (!listaPublishersLocais.ContainsKey(evento.getNomePublisher()))
                                    listaPublishersLocais.Add(evento.getNomePublisher(), 0);

                                if (evento.getSeqNumberSite() == listaPublishersLocais[evento.getNomePublisher()] + 1)
                                {
                                    for (int i = 0; i < tabelaSubscricoes.Count; i++)
                                    {
                                        Subscricao subscricao = tabelaSubscricoes[i];
                                        /*delEnviaFiltering del1 = new delEnviaFiltering(enviaSubscriberFloodingFIFO);
                                        funcaoCallBack = new AsyncCallback(OnExit11);
                                        IAsyncResult ar =del1.BeginInvoke(evento, subscricao, funcaoCallBack, null);*/
                                        if (evento.getQuemEnviouEvento().Equals("publisher")) //envio do ACK para publishers
                                        {

                                            IPublisher pubs = (IPublisher)Activator.GetObject(typeof(IPublisher), urlQuemEnviouEvento);
                                            delACK del1 = new delACK(pubs.recebeACK);
                                            funcaoCallBack = new AsyncCallback(OnExit);
                                            IAsyncResult ar1 =del1.BeginInvoke(evento.getSeqNumber(), funcaoCallBack, null);
                                        }
                                        if (evento.getQuemEnviouEvento().Equals("broker")) //envio do ACK para brokers
                                        {
                                            try
                                            {
                                                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                                                delACK2 del2 = new delACK2(bro.recebeAckBrokers);
                                                funcaoCallBack = new AsyncCallback(OnExit);
                                                IAsyncResult ar1 =del2.BeginInvoke(evento.getSeqNumber(), site, evento.getNomePublisher(), funcaoCallBack, null);
                                            }
                                            catch (Exception e) { }
                                        }
                                        enviaSubscriberFloodingSincrono(evento, subscricao);
                                    }

                                    listaPublishersLocais[evento.getNomePublisher()]++;

                                    // verificaEsperaFlooding();
                                    delConfirmacao del = new delConfirmacao(verificaEsperaFIFOFlooding);
                                    funcaoCallBack = new AsyncCallback(OnExit14);
                                    IAsyncResult ar =del.BeginInvoke(funcaoCallBack, null);
                                }
                                else
                                {

                                    listaEventosEspera.Add(evento);
                                }
                            }
                        }
                        else //caso caso é NO ordering
                            for (int i = 0; i < tabelaSubscricoes.Count; i++)
                            {
                                Subscricao subscricao = tabelaSubscricoes[i];
                                delEnviaFiltering del = new delEnviaFiltering(enviaSubscriberFloodingAssincrono);
                                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit11);
                                IAsyncResult ar =del.BeginInvoke(evento, subscricao, funcaoCallBack, null);

                                //enviaSubscriberFloodingNO(evento, subscricao);
                            }

                        if (evento.getQuemEnviouEvento().Equals("broker")) //envio do ACK para brokers
                        {
                            try
                            {
                                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                                delACK2 del = new delACK2(bro.recebeAckBrokers);
                                funcaoCallBack = new AsyncCallback(OnExit);
                                IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), site, evento.getNomePublisher(), funcaoCallBack, null);
                            }
                            catch (Exception e) { }
                        }
                    }
                    
                }
                else if (routingPolicy.Equals("filtering"))
                {

                    lock (lockEncaminhamento)
                    {
                        if (!podeReceberEventos) //se esta condiçao der falso, quer dizer que as tabelas de encaminhamento ainda estao a ser processadas e os eventos ainda nao podem ser processados (menos no caso do flooding)                                                 
                            Monitor.Wait(lockEncaminhamento);                      
                    }

                    lock (lockEvento)
                    {
                        if (lockEventos)
                        {
                            if (evento.getQuemEnviouEvento().Equals("publisher")) //envio do ACK para publishers
                            {

                                IPublisher pubs = (IPublisher)Activator.GetObject(typeof(IPublisher), urlQuemEnviouEvento);
                                delACK del = new delACK(pubs.recebeACK);
                                funcaoCallBack = new AsyncCallback(OnExit);
                                IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), funcaoCallBack, null);
                            }
                            if (evento.getQuemEnviouEvento().Equals("broker")) //envio do ACK para brokers
                            {
                                try
                                {
                                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                                    delACK2 del = new delACK2(bro.recebeAckBrokers);
                                    funcaoCallBack = new AsyncCallback(OnExit);
                                    IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), site, evento.getNomePublisher(), funcaoCallBack, null);
                                }
                                catch (Exception e) { }
                            }
                            Monitor.Wait(lockEvento);
                        }
                    }

                    if (orderingPolicy.Equals("TOTAL"))
                    {
                        lock (lock_3)
                        {
                            if (!listaPublishersLocais.ContainsKey(evento.getNomePublisher()))
                                listaPublishersLocais.Add(evento.getNomePublisher(), 0);

                            if (evento.getSeqNumberSite() == listaPublishersLocais[evento.getNomePublisher()] + 1)
                            {

                                Pedido pedido = new Pedido(getUrl(), 0, numeroPedido, getID());   //Cria o pedido
                                pedido.setPublisher(evento.getNomePublisher());
                                pedido.setSeqNumber(evento.getSeqNumber());
                                pedido.setSite(site);


                                for(int i = 0; i < tabelaSubscricoes.Count; i++ )
                                {
                                    Subscricao subscricao = tabelaSubscricoes[i];
                                    string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
                                    if (topicos.Last().Equals("*"))
                                    {
                                        int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                                        string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                                        if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                                            pedido.addSubscritor(subscricao.getIDSubscriber(), 0);      //Colocamos no dictionary que vai no pedido para o sequenciador atribuir um numero por subscriber para poder ser organizado no broker final

                                    }
                                    else if (evento.getTopic().Equals(subscricao.getEvento().getTopic()))
                                        pedido.addSubscritor(subscricao.getIDSubscriber(), 0);  //Colocamos no dictionary que vai no pedido para o sequenciador atribuir um numero por subscriber para poder ser organizado no broker final

                                }

                                if (!urlBrokerPai.Equals("raiz"))        //Se este broker não for a raiz, temos de enviar um pedido à raiz para saber o sequencer
                                {
                                    lock(lock_3)
                                         listaPedidoEspera.Add(pedido);

                                    delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                    IAsyncResult ar =del3.BeginInvoke(pedido, this.url,"raiz", "", funcaoCallBack, null);

                                    try
                                    {
                                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);

                                        delSequencer del = new delSequencer(bro.pedidoSequencer);
                                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                        IAsyncResult ar1 =del.BeginInvoke(pedido, this.url, funcaoCallBack, null);
                                    }catch(Exception e) { }
                                }
                                else   //Caso contrario, este broker é a raiz (i.e. o publisher encontra-se aqui), logo perguntamos localmente o sequencer
                                {
                                    delSequencer del = new delSequencer(pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar =del.BeginInvoke(pedido,urlQuemEnviouEvento, funcaoCallBack, null);
                                }

                                eventosEspera_pedido.Add(numeroPedido, evento);     //Adiciona o evento a um dictionary com o numero de pedido associado

                                numeroPedido++;
                                listaPublishersLocais[evento.getNomePublisher()]++;
                                verificaFilaEsperaTotal();
                            }
                            else
                                listaEventosEspera.Add(evento);
                        }
                    }
                   else  if (orderingPolicy.Equals("FIFO"))
                    {

                        lock (lockEventosEspera)
                        {
                            if (!listaPublishersLocais.ContainsKey(evento.getNomePublisher()))
                                listaPublishersLocais.Add(evento.getNomePublisher(), 0);
                            if (evento.getSeqNumberSite() == listaPublishersLocais[evento.getNomePublisher()] + 1)
                            {
                                for(int i = 0; i < tabelaSubscricoes.Count; i++ )
                                {
                                    Subscricao subscricao = tabelaSubscricoes[i];
                                    /*delEnviaFiltering del1 = new delEnviaFiltering(enviaSubscriberFilteringFIFO);
                                    funcaoCallBack = new AsyncCallback(OnExit11);
                                    IAsyncResult ar =del1.BeginInvoke(evento, subscricao, funcaoCallBack, null);*/
                                    if (evento.getQuemEnviouEvento().Equals("publisher")) //envio do ACK para publishers
                                    {

                                        IPublisher pubs = (IPublisher)Activator.GetObject(typeof(IPublisher), urlQuemEnviouEvento);
                                        delACK del = new delACK(pubs.recebeACK);
                                        funcaoCallBack = new AsyncCallback(OnExit);
                                        IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), funcaoCallBack, null);
                                    }
                                    if (evento.getQuemEnviouEvento().Equals("broker")) //envio do ACK para brokers
                                    {
                                        try
                                        {
                                            IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                                            delACK2 del = new delACK2(bro.recebeAckBrokers);
                                            funcaoCallBack = new AsyncCallback(OnExit);
                                            IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), site, evento.getNomePublisher(), funcaoCallBack, null);
                                        }
                                        catch (Exception e) { }
                                    }

                                    enviaSubscriberFilteringFIFO(evento, subscricao);
                                }
                                listaPublishersLocais[evento.getNomePublisher()]++;
                                verificaEsperaFiltering();

                            }
                            else
                            {
                                lock (lockEventosEspera)
                                     listaEventosEspera.Add(evento);
                            }
                        }
                    }
                    else if(orderingPolicy.Equals("NO"))
                    {
                        for (int i = 0; i < tabelaSubscricoes.Count; i++)
                        {
                            Subscricao subscricao = tabelaSubscricoes[i];

                            delEnviaFiltering del = new delEnviaFiltering(enviaSubscriberFilteringNO);
                            AsyncCallback funcaoCallBack = new AsyncCallback(OnExit11);
                            IAsyncResult ar =del.BeginInvoke(evento, subscricao, funcaoCallBack, null);

                            // enviaSubscriberFilteringNO(evento, subscricao);
                        }
                    }
                    
                }
            }

            if (evento.getQuemEnviouEvento().Equals("publisher")) //envio do ACK para publishers
            {

                IPublisher pubs = (IPublisher)Activator.GetObject(typeof(IPublisher), urlQuemEnviouEvento);
                delACK del = new delACK(pubs.recebeACK);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), funcaoCallBack, null);
            }
            if (evento.getQuemEnviouEvento().Equals("broker")) //envio do ACK para brokers
            {
                try
                {
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                    delACK2 del = new delACK2(bro.recebeAckBrokers);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), site, evento.getNomePublisher(), funcaoCallBack, null);
                }
                catch(Exception e) { }
            }
        }

        public void recebeEventoTotal(Evento evento, string urlQuemEnviouEvento)        //TOTAL - Função usada para propagar os eventos e entregar aos subscriber FLOODING
        {
            string siteParaACK = evento.getSite();

            if (freezeAtivo)
            {
                Monitor.Wait(this);
                if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                    return;
                else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                {
                    KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                    IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                    delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                    funcaoCallBack = new AsyncCallback(OnExit4);
                    IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                    return;
                }
            }

            if (!urlBrokerPai.Equals("raiz") && !urlQuemEnviouEvento.Equals(urlBrokerPai)) // se nao for a raiz, propagamos pelo pai, se o URL vier do Pai, também nao propagamos de volta para ele
            {
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), getUrlPai());
                if (loggingLevel.Equals("full"))
                {
                    IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                    pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), "");
                }

                lock(lock_2)
                    listaEsperaTotalACK.Add(evento);

                delRemove del1 = new delRemove(verificaEntregaCompletaRecebeTotal);
                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit17);
                IAsyncResult ar =del1.BeginInvoke(evento, "pai", funcaoCallBack, null);

                try
                {
                    delRecebeTotal del = new delRecebeTotal(bro.recebeEventoTotal);
                    funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                    IAsyncResult ar1 =del.BeginInvoke(evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                }
                catch(Exception e) { }
            }

            foreach (BrokerFilho bro in listaFilhos) //propaga o evento para os filhos
            {
                IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), bro.getBroker());

                if (!urlQuemEnviouEvento.Equals(broFilho.getUrl())) //no caso em que propagamos para o filho, nao propagamos para o filho que enviou para o pai
                {
                    if (loggingLevel.Equals("full"))
                    {
                        IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                        pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), "");
                    }
                    lock(lock_2)
                        listaEsperaTotalACK.Add(evento);

                    delRemove del1 = new delRemove(verificaEntregaCompletaRecebeTotal);
                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit17);
                    IAsyncResult ar =del1.BeginInvoke(evento, "filho", funcaoCallBack, null);

                    try
                    {
                        delRecebeTotal del = new delRecebeTotal(broFilho.recebeEventoTotal);
                        funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                        IAsyncResult ar1 =del.BeginInvoke(evento, getUrl(), funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                    }catch(Exception e) { }
                }
            }

            lock (lock_5)
            {
                if (evento.getSequencer() == sequencer + 1)
                {
                    foreach (Subscricao subscricao in tabelaSubscricoes)
                        enviaSubscriberFloodingSincrono(evento, subscricao);

                    sequencer++;
                    verificaEsperaEventoSubscriber();
                }
                else
                    listaEventosEspera2.Add(evento);
            }

            try
            {
                //enviamos o ACK para o broker que enviou o evento
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviouEvento);
                delACK2 del = new delACK2(bro.recebeAckPropagacao);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), siteParaACK, evento.getNomePublisher(), funcaoCallBack, null);
            }
            catch (Exception e) { }
        }

        public void pedidoSequencer(Pedido pedido, string urlQuemEnviou)      //Função para pedir à raiz um sequencer
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            if (pedido.getSequencer() == 0)     //Se o sequencer for 0, quer dizer que estamos a perguntar à raiz, quer dizer que estamos a subir na árvore
            {
                if (!urlBrokerPai.Equals("raiz"))    //Se ainda não estivermos na raiz, continuamos a propagar para o pai
                {
                    //Propagamos sempre para o pai para chegar à raiz
                    lock(lock_3)
                        listaPedidoEspera.Add(pedido);

                    delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                    IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "raiz", "", funcaoCallBack, null);
                    try
                    {
                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);

                        delSequencer del = new delSequencer(bro.pedidoSequencer);
                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                        IAsyncResult ar1 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                    }catch(Exception e) { }
                }
                else    //Quando chegamos à raiz
                {
                    lock (lock_7)    //lock_ usado para todos os pedidos na raiz
                    {
                        if (!pedidosSequencer.ContainsKey(pedido.getUrlOrigem()))       //Se é a primeira vez que estamos a receber um pedido deste broker, adicionamo-lo ao dictionary
                            pedidosSequencer.Add(pedido.getUrlOrigem(), 1);

                        if (routingPolicy.Equals("flooding"))
                        {
                            if (pedidosSequencer[pedido.getUrlOrigem()] == pedido.getNumeroPedido())    //Vemos se é este o próximo pedido do broker
                            {
                                sequencerRaiz++;
                                pedido.setSequencer(sequencerRaiz);     //Colocamos o sequenciador no pedido para mandá-lo de volta

                                //Vamos começar a propagar para o broker que pediu
                                if (tabelaEncaminhamento.ContainsKey(pedido.getIdBrokerOrigem()))
                                {
                                    lock (lock_3)
                                       listaPedidoEspera.Add(pedido);

                                    delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                    IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "",pedido.getIdBrokerOrigem(), funcaoCallBack, null);

                                    try
                                    {
                                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[pedido.getIdBrokerOrigem()]);
                                        delSequencer del = new delSequencer(bro.pedidoSequencer);
                                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                        IAsyncResult ar1 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                    }catch(Exception e){}
                                }
                                else
                                {
                                    delSequencer del = new delSequencer(pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar =del.BeginInvoke(pedido, urlQuemEnviou, funcaoCallBack, null);
                                }


                                pedidosSequencer[pedido.getUrlOrigem()]++;      //Incrementamos o valor do pedido na tabela local, para estarmos à espera do próximo pedido desse broker

                                verificaFilaEsperaPedidos(pedido.getUrlOrigem(), pedido.getIdBrokerOrigem());   //Verificamos se estava algum pedido à espera
                            }
                            else    //Entra na fila de espera
                                filaPedidosEspera.Add(pedido);
                        }
                        else //filtering
                        {
                            if (pedidosSequencer[pedido.getUrlOrigem()] == pedido.getNumeroPedido())    //Vemos se é este o próximo pedido do broker
                            {
                                List<string> aux = pedido.getSubscritores().Keys.ToList();

                                foreach (string idSub in aux)
                                {
                                    if (!sequencerSubscritores.ContainsKey(idSub))
                                        sequencerSubscritores.Add(idSub, 0);

                                    sequencerSubscritores[idSub]++;
                                    pedido.getSubscritores()[idSub] = sequencerSubscritores[idSub];
                                }

                                pedido.setSequencer(1);

                                lock (lock_3)
                                       listaPedidoEspera.Add(pedido);

                                delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "", pedido.getIdBrokerOrigem(), funcaoCallBack, null);

                                //Vamos começar a propagar para o broker que pediu
                                if (tabelaEncaminhamento.ContainsKey(pedido.getIdBrokerOrigem()))
                                {
                                    try
                                    {
                                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[pedido.getIdBrokerOrigem()]);
                                        delSequencer del = new delSequencer(bro.pedidoSequencer);
                                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                        IAsyncResult ar1 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                    }catch(Exception e) { }
                                }
                                else  //Se estiver na raiz
                                {
                                    delSequencer del = new delSequencer(pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar1 =del.BeginInvoke(pedido,urlQuemEnviou, funcaoCallBack, null);
                                }

                                pedidosSequencer[pedido.getUrlOrigem()]++;      //Incrementamos o valor do pedido na tabela local, para estarmos à espera do próximo pedido desse broker

                                verificaFilaEsperaPedidos(pedido.getUrlOrigem(), pedido.getIdBrokerOrigem());   //Verificamos se estava algum pedido à espera
                            }
                            else
                                filaPedidosEspera.Add(pedido);
                        }
                    }
                }
            }
            else      //Se o sequencer já não for 0, quer dizer que a raiz já deu um valor ao sequencer, e estamos a descer na árvore
            {
                if (!pedido.getUrlOrigem().Equals(getUrl()))  //Enquanto não chegarmos ao broker que originou o pedido do sequencer, continuamos a propagar
                {
                    lock (lock_3)
                        listaPedidoEspera.Add(pedido);

                    delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                    IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "", pedido.getIdBrokerOrigem(), funcaoCallBack, null);

                    //Continuamos a propagar até chegar ao broker que pediu
                    try
                    {
                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[pedido.getIdBrokerOrigem()]);
                        delSequencer del = new delSequencer(bro.pedidoSequencer);

                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                        IAsyncResult ar1 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                    }catch(Exception e) { }
                }
                else        //Se já chegámos ao broker inicial que fez o pedido
                {
                    lock (lock_2)
                        filaPedidosRecebidos.Add(pedido);

                    if (routingPolicy.Equals("flooding"))
                        verificaChegadaPedidoFlooding();
                    else //filtering
                    {
                        verificaChegadaPedidoFiltering();
                    }
                }
            }
            try
            {
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviou);
                delPedidoACK del2 = new delPedidoACK(bro.recebeACKPedidoSequencer);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del2.BeginInvoke(pedido.getSeqNumber(),pedido.getSite(),pedido.getNomePublisher(), funcaoCallBack, null);
            }
            catch (Exception e) { }
        }

        public void verificaEntregaCompletaTotal(Pedido pedido, string urlQuemEnviou, string nodeAEnviar, string idBrokerDestino)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            Thread.Sleep(10000); //se passar X tempo e o pedido nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           

            lock(lock_3)
            {
                try
                {
                    if (listaPedidoEspera.Any(x => x.getSeqNumber() == pedido.getSeqNumber() && x.getSite().Equals(pedido.getSite()) && x.getNomePublisher().Equals(pedido.getNomePublisher())))
                    {
                        if (nodeAEnviar.Equals("raiz"))
                        {
                            lock (lockSentinela)
                            {
                                if (!sentinela)
                                {
                                    sentinela = true;

                                    KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                                    listaBrokersPais[urlBrokerAtivo.Key] = "SUSPEITO";

                                    KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                    listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";
                                    urlBrokerPai = urlNovoBrokerAtivo.Key;

                                    try
                                    {
                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                        delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                        funcaoCallBack = new AsyncCallback(OnExit3);
                                        IAsyncResult ar1 = del.BeginInvoke(urlBrokerAtivo.Key, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                    }
                                    catch (Exception e) { }
                                }

                                delTotal del1 = new delTotal(reenviaPedido);
                                AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit15);
                                IAsyncResult ar = del1.BeginInvoke(pedido, urlQuemEnviou, "raiz", "", funcaoCallBack1, null);
                            }
                        }
                        else
                        {
                            lock (lockSentinela)
                            {
                                if (!sentinela)
                                {
                                    sentinela = true;
                                    //vamos ver se é um broker pai ou filho, porque neste caso nao há a distinção
                                    //se vamos enviar para um pai ou filho, por isso precisamos de saber para qual vamos 
                                    //enviar para ir buscar o seu irmao
                                    string urlBrokerSuspeito = tabelaEncaminhamento[idBrokerDestino];

                                    //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 

                                    if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                                    {
                                        listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";

                                        KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                        listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";

                                        tabelaEncaminhamento[idBrokerDestino] = urlNovoBrokerAtivo.Key; //atualizamos a tabela de encaminhamento

                                        try
                                        {
                                            IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                            delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                            funcaoCallBack = new AsyncCallback(OnExit3);
                                            IAsyncResult ar1 = del.BeginInvoke(urlBrokerSuspeito, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                        }
                                        catch (Exception e) { }
                                    }
                                    // se isto se verificar então ia ser enviado para um broker filho
                                    else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                                    {
                                        //vamos buscar o filho suspeito
                                        BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));
                                        brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                        string urlNovoPrincipal = brokerSuspeito.novoLider(urlBrokerSuspeito); //é atualizado o novo broker principal e é recebido o seu url
                                        tabelaEncaminhamento[idBrokerDestino] = urlNovoPrincipal; //atualizamos a tabela de encaminhamento

                                        try
                                        {
                                            IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoPrincipal);
                                            delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                            funcaoCallBack = new AsyncCallback(OnExit3);
                                            IAsyncResult ar1 = del.BeginInvoke(urlBrokerSuspeito, urlNovoPrincipal, this.url, funcaoCallBack, null);
                                        }
                                        catch (Exception e) { }
                                    }
                                }

                                delTotal del1 = new delTotal(reenviaPedido);
                                AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit15);
                                IAsyncResult ar = del1.BeginInvoke(pedido, urlQuemEnviou, "", idBrokerDestino, funcaoCallBack1, null);
                            }
                        }
                    }
                }catch(Exception e) { }
            }
        }

        public void reenviaPedido(Pedido pedido, string urlQuemEnviou, string nodeAEnviar, string idBrokerDestino)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            lock (lock_)
            {
                if (ajudaLock)
                    Monitor.Wait(lock_);
            }

            if (nodeAEnviar.Equals("raiz"))
            {
                KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                if (!urlQuemEnviou.Equals(urlBrokerAtivo.Key))
                {
                    try
                    {
                        IBroker objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);
                        delSequencer del = new delSequencer(objetoBroker.pedidoSequencer); //reenviamos o pedido do sequencer
                        funcaoCallBack = new AsyncCallback(OnExit16);
                        IAsyncResult ar =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                    }
                    catch (Exception e) { }
                }
                else
                    lock (lock_3)
                         listaPedidoEspera.RemoveAll(x => x.getSeqNumber() == pedido.getSeqNumber() && x.getSite().Equals(pedido.getSite()) && x.getNomePublisher().Equals(pedido.getNomePublisher()));
            }
            else
            {
                try
                {
                    IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idBrokerDestino]);
                    if(!urlQuemEnviou.Equals(broker.getUrl()))
                    {
                        delSequencer del = new delSequencer(broker.pedidoSequencer);
                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                        IAsyncResult ar =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                    }
                    else
                        lock(lock_3)
                         listaPedidoEspera.RemoveAll(x => x.getSeqNumber() == pedido.getSeqNumber() && x.getSite().Equals(pedido.getSite()) && x.getNomePublisher().Equals(pedido.getNomePublisher()));

                }
                catch (Exception e) { }
            }

            Thread.Sleep(10000);

            lock(lock_3)
            {
                if (listaPedidoEspera.Any(x => x.getSeqNumber() == pedido.getSeqNumber() && x.getSite().Equals(pedido.getSite()) && x.getNomePublisher().Equals(pedido.getNomePublisher())))
                {
                    lock (lockSentinela)
                    {
                        if (!sentinela2)
                        {
                            sentinela = false;
                            sentinela2 = true;
                        }

                        delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                        IAsyncResult ar =del3.BeginInvoke(pedido, urlQuemEnviou, nodeAEnviar, idBrokerDestino, funcaoCallBack, null);
                    }                                 
                }
                else
                    lock (lockSentinela)
                        sentinela2 = false;
            }
        }

        public void verificaChegadaPedidoFlooding()   //Para ser usado quando o pedido chegou de volta com o sequenciador já
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_2)
            {
                for (int i = 0; i < filaPedidosRecebidos.Count; i++)
                {
                    Pedido pedido = filaPedidosRecebidos[i];
                    try
                    {
                        if (eventosEspera_pedido.Keys.First() == pedido.getNumeroPedido())      //Se for este o pedido que estávamos à espera
                        {
                            Evento evento = eventosEspera_pedido[pedido.getNumeroPedido()];
                            evento.setSequencer(pedido.getSequencer());     //Colocamos o sequencer no evento

                            if (!urlBrokerPai.Equals("raiz")) // se nao for a raiz, propagamos para o pai
                            {
                                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);

                                if (loggingLevel.Equals("full"))
                                {
                                    IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                                    pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), "");
                                }

                                listaEsperaTotalACK.Add(evento);

                                delRemove del1 = new delRemove(verificaEntregaCompletaRecebeTotal);
                                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit17);
                                IAsyncResult ar =del1.BeginInvoke(evento, "pai", funcaoCallBack, null);

                                try
                                {
                                    delRecebeTotal del = new delRecebeTotal(bro.recebeEventoTotal);
                                    funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                                    IAsyncResult ar1 =del.BeginInvoke(evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                                }
                                catch(Exception e) { }
                            }

                            foreach (BrokerFilho bro in listaFilhos) //propaga o evento para os filhos
                            {
                                if (loggingLevel.Equals("full"))
                                {
                                    IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                                    pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), "");
                                }

                                listaEsperaTotalACK.Add(evento);

                                delRemove del1 = new delRemove(verificaEntregaCompletaRecebeTotal);
                                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit17);
                                IAsyncResult ar =del1.BeginInvoke(evento, "filho", funcaoCallBack, null);

                                IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), bro.getBroker());

                                try
                                {
                                    delRecebeTotal del = new delRecebeTotal(broFilho.recebeEventoTotal);
                                    funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                                    IAsyncResult ar1 =del.BeginInvoke(evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                                }catch(Exception e) { }
                            }

                            //Para o broker entregar num subscriber que esteja aqui ligado
                            if (evento.getSequencer() == sequencer + 1)
                            {
                                foreach (Subscricao subscricao in tabelaSubscricoes)
                                    enviaSubscriberFloodingSincrono(evento, subscricao);

                                sequencer++;
                                verificaEsperaEventoSubscriber();
                            }
                            else
                                listaEventosEspera2.Add(evento);

                            eventosEspera_pedido.Remove(eventosEspera_pedido.Keys.First());      //Remove o evento da fila de espera do broker (evento estava à espera do sequencer)
                            filaPedidosRecebidos.Remove(pedido);

                            verificaChegadaPedidoFlooding();
                            break;
                        }
                    }
                    catch (Exception e) { }

                }
            }

        }

        public void verificaEntregaCompletaRecebeTotal(Evento evento, string nodeAEnviar)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           

            lock (lock_2)
            {
                try
                {
                    if (listaEsperaTotalACK.Any(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(evento.getSite()) && x.getNomePublisher().Equals(evento.getNomePublisher())))
                    {
                        if (nodeAEnviar.Equals("pai"))
                        {
                            lock (lockSentinela)
                            {
                                if (!sentinela3)
                                {
                                    sentinela3 = true;
                                    KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                                    listaBrokersPais[urlBrokerAtivo.Key] = "SUSPEITO";

                                    KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                    listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";
                                    urlBrokerPai = urlNovoBrokerAtivo.Key;
                                    try
                                    {
                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                        delRegistoBroker del2 = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                        funcaoCallBack = new AsyncCallback(OnExit3);
                                        IAsyncResult ar1 =del2.BeginInvoke(urlBrokerAtivo.Key, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                    }
                                    catch (Exception e) { }
                                }
                                delRecebeTotal del = new delRecebeTotal(reenviaRecebeTotal);
                                funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                                IAsyncResult ar =del.BeginInvoke(evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                            }
                        }
                        else if (nodeAEnviar.Equals("filho"))
                        {
                            lock (lockSentinela)
                            {
                                if (!sentinela)
                                {
                                    sentinela = true;

                                    BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));
                                    string urlBrokerSuspeito = broker.getBroker();
                                    broker.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito

                                    string urlNovoPrincipal = broker.novoLider(urlBrokerSuspeito); //é atualizado o novo broker principal e é recebido o seu url                          

                                    try
                                    {
                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoPrincipal);
                                        delRegistoBroker del1 = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                        funcaoCallBack = new AsyncCallback(OnExit3);
                                        IAsyncResult ar1 =del1.BeginInvoke(urlBrokerSuspeito, urlNovoPrincipal, this.url, funcaoCallBack, null);
                                    }
                                    catch (Exception e) { }
                                }
                                delRecebeTotal del = new delRecebeTotal(reenviaRecebeTotal);
                                funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                                IAsyncResult ar =del.BeginInvoke(evento, nodeAEnviar, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                            }
                        }
                    }
                }catch(Exception e) { }
            }
        }

        public void reenviaRecebeTotal(Evento evento, string nodeAEnviar)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            lock (lock_)
            {
                if (ajudaLock)
                    Monitor.Wait(lock_);
            }

            if (nodeAEnviar.Equals("filho"))
            {
                try
                {
                    BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));
                    IBroker objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), broker.getBroker());

                    delRecebeTotal del = new delRecebeTotal(objetoBroker.recebeEventoTotal);
                    funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                    IAsyncResult ar =del.BeginInvoke(evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                }
                catch (Exception e) { }
            }
            else if(nodeAEnviar.Equals("pai"))
            {
                KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito

                try
                {
                    IBroker objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);
                    delRecebeTotal del = new delRecebeTotal(objetoBroker.recebeEventoTotal);
                    funcaoCallBack = new AsyncCallback(OnExitRecebeTotal);
                    IAsyncResult ar =del.BeginInvoke(evento, this.url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
                }
                catch(Exception e) { }
            }

            Thread.Sleep(10000);

            lock(lock_2)
            {
                if (listaEsperaTotalACK.Any(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(evento.getSite()) && x.getNomePublisher().Equals(evento.getNomePublisher())))
                {
                    lock (lockSentinela)
                    {
                        if(!sentinela2)
                        {
                            sentinela = true;
                            sentinela2 = false;
                            sentinela3 = true;
                        }
                    }
                    delRemove del1 = new delRemove(verificaEntregaCompletaRecebeTotal);
                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit17);
                    IAsyncResult ar =del1.BeginInvoke(evento, "pai", funcaoCallBack, null);
                }
                else
                    lock(lockSentinela)
                        sentinela2 = false;
            }
        }

        public void verificaEsperaEventoSubscriber()        //TOTAL - Fila de espera no broker que está para entregar ao subscriber
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_5)
            {
                for(int i = 0; i<listaEventosEspera2.Count; i++)
                {
                    Evento evento = listaEventosEspera2[i];
                    if (evento.getSequencer() == sequencer + 1)
                    {
                        foreach (Subscricao subscricao in tabelaSubscricoes)
                            enviaSubscriberFloodingSincrono(evento, subscricao);

                        sequencer++;
                        verificaEsperaEventoSubscriber();
                        break;
                    }
                }
            }
        }

        public void verificaChegadaPedidoFiltering()
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_2)
            {
                for(int i = 0; i < filaPedidosRecebidos.Count; i++)
                {
                    Pedido pedido = filaPedidosRecebidos[i];
                    if (eventosEspera_pedido.Keys.First() == pedido.getNumeroPedido())      //Se for este o pedido que estávamos à espera
                    {
                        Evento evento = eventosEspera_pedido[pedido.getNumeroPedido()];

                        foreach (Subscricao subscricao in tabelaSubscricoes)
                            enviaSubscriberFilteringTOTAL(evento, subscricao, pedido);

                        eventosEspera_pedido.Remove(eventosEspera_pedido.Keys.First());      //Remove o evento da fila de espera do broker (evento estava à espera do sequencer)
                        filaPedidosRecebidos.Remove(pedido);

                        verificaChegadaPedidoFiltering();
                        break;
                    }
                }
            }
        }

        public void verificaFilaEsperaPedidos(string urlQuemPediu, string idQuemPediu)      //Para a raiz usar para organizar os pedidos recebidos
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_7)
            {
                for(int i = 0; i<filaPedidosEspera.Count; i++)
                {
                    Pedido pedido = filaPedidosEspera[i];

                    if (pedido.getUrlOrigem().Equals(urlQuemPediu))
                    {
                        if (routingPolicy.Equals("flooding"))
                        {
                            if (pedido.getNumeroPedido() == pedidosSequencer[urlQuemPediu])
                            {
                                sequencerRaiz++;
                                pedido.setSequencer(sequencerRaiz); //Colocamos o sequenciador no evento e vamos propagá-lo de volta ao broker que pediu

                                //Vamos começar a propagar para o broker que pediu
                                if (tabelaEncaminhamento.ContainsKey(tabelaEncaminhamento[idQuemPediu]))
                                {
                                    lock (lock_3)
                                       listaPedidoEspera.Add(pedido);

                                    delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                    IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "", idQuemPediu, funcaoCallBack, null);

                                    try
                                    {
                                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idQuemPediu]);
                                        delSequencer del = new delSequencer(bro.pedidoSequencer);
                                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                        IAsyncResult ar1 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                    }
                                    catch(Exception e) { }
                                }
                                else
                                {
                                    delSequencer del = new delSequencer(pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                }

                                pedidosSequencer[urlQuemPediu]++;       //Incrementamos o valor de pedidos que já recebemos deste broker para estar à espera do próximo pedido dele
                                verificaFilaEsperaPedidos(urlQuemPediu, idQuemPediu);
                                break;
                            }
                        }
                        else
                        {
                            if (pedido.getNumeroPedido() == pedidosSequencer[urlQuemPediu])    //Vemos se é este o próximo pedido do broker
                            {
                                List<string> aux = pedido.getSubscritores().Keys.ToList();
                                foreach (string idSub in aux)
                                {
                                    if (!sequencerSubscritores.ContainsKey(idSub))
                                        sequencerSubscritores.Add(idSub, 0);

                                    sequencerSubscritores[idSub]++;
                                    pedido.getSubscritores()[idSub] = sequencerSubscritores[idSub];
                                }

                                pedido.setSequencer(1); //É preciso colocar um valor acima de 0 para ser propagado para o broker de origem, pois quando está a 0 está a subir para a raiz


                                //Vamos começar a propagar para o broker que pediu
                                if (tabelaEncaminhamento.ContainsKey(idQuemPediu))
                                {
                                    lock (lock_3)
                                       listaPedidoEspera.Add(pedido);

                                    delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                    IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "", idQuemPediu, funcaoCallBack, null);

                                    try
                                    {
                                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idQuemPediu]);
                                        delSequencer del = new delSequencer(bro.pedidoSequencer);
                                        funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                        IAsyncResult ar1 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                    }catch(Exception e) { }
                                }
                                else
                                {
                                    delSequencer del = new delSequencer(pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                }

                                pedidosSequencer[urlQuemPediu]++;      //Incrementamos o valor do pedido na tabela local, para estarmos à espera do próximo pedido desse broker
                                verificaFilaEsperaPedidos(urlQuemPediu, idQuemPediu);
                                break;
                            }
                        }
                    }
                }
            }
        }

        public void verificaEsperaFIFOFlooding() //FIFO - No broker final para entregar ao subscriber
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lockEventosEspera)
            {
                for (int i = 0; i<listaEventosEspera.Count; i++) 
                {
                    Evento evento = listaEventosEspera[i];

                    int seqNumberSite = listaPublishersLocais[evento.getNomePublisher()];

                    if (evento.getSeqNumberSite() == seqNumberSite + 1)
                    {

                        foreach (Subscricao subscricao in tabelaSubscricoes)
                        {
                            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
                            if (topicos.Last().Equals("*"))
                            {
                                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                                string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                                    enviaSubscriberFloodingSincrono(evento, subscricao);
                            }
                            else if (subscricao.getEvento().getTopic().Equals(evento.getTopic())) //se der true, entao este subscritor está subscrito a este evento e recebe-o
                                enviaSubscriberFloodingSincrono(evento, subscricao);
                        }

                        listaPublishersLocais[evento.getNomePublisher()]++;
                        listaEventosEspera.Remove(evento);
                        verificaEsperaFIFOFlooding();
                        break;
                    }
                }
            }
        }  

        public void verificaFilaEsperaTotal()       //TOTAL - Lista de espera Publisher-Broker para depois fazer o pedido do sequencer
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_3)
            {
                for (int i = 0; i < listaEventosEspera.Count; i++)
                {
                    Evento evento = listaEventosEspera[i];
                    if (routingPolicy.Equals("flooding"))
                    {
                        if (evento.getSeqNumberSite() == listaPublishersLocais[evento.getNomePublisher()] + 1)
                        {
                            Pedido pedido = new Pedido(getUrl(), 0, numeroPedido, getID());   //Cria o pedido

                            if (!urlBrokerPai.Equals("raiz"))    //Se este broker não for a raiz, irá começar a propagar para cima para pedir à raiz o sequenciador
                            {
                                listaPedidoEspera.Add(pedido);

                                delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "raiz", "", funcaoCallBack, null);

                                try
                                {
                                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                                    delSequencer del = new delSequencer(bro.pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar11 =del.BeginInvoke(pedido, this.url, funcaoCallBack, null);
                                }catch(Exception e) { }
                            }
                            else    //Se este broker é já a raiz (se o publisher está na raiz, invocamos a função para obter o sequenciador aqui no proprio broker)
                                pedidoSequencer(pedido, this.url);

                            eventosEspera_pedido.Add(numeroPedido, evento);     //Adiciona o evento a um dictionary com o numero de pedido associado

                            listaEventosEspera.Remove(evento);

                            numeroPedido++;
                            listaPublishersLocais[evento.getNomePublisher()]++;
                            verificaFilaEsperaTotal();
                            break;
                        }
                    }
                    else //filtering
                    {
                        if (evento.getSeqNumberSite() == listaPublishersLocais[evento.getNomePublisher()] + 1)
                        {
                            Pedido pedido = new Pedido(getUrl(), 0, numeroPedido, getID());   //Cria o pedido

                            foreach (Subscricao subscricao in tabelaSubscricoes)
                            {
                                string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
                                if (topicos.Last().Equals("*"))
                                {
                                    int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                                    string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                                    if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                                        pedido.addSubscritor(subscricao.getIDSubscriber(), 0);      //Colocamos no dictionary que vai no pedido para o sequenciador atribuir um numero por subscriber para poder ser organizado no broker final
                                }
                                else if (evento.getTopic().Equals(subscricao.getEvento().getTopic()))
                                    pedido.addSubscritor(subscricao.getIDSubscriber(), 0);  //Colocamos no dictionary que vai no pedido para o sequenciador atribuir um numero por subscriber para poder ser organizado no broker final
                            }

                            if (!urlBrokerPai.Equals("raiz"))    //Se este broker não for a raiz, irá começar a propagar para cima para pedir à raiz o sequenciador
                            {
                               
                                listaPedidoEspera.Add(pedido);

                                delTotal del3 = new delTotal(verificaEntregaCompletaTotal);
                                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit15);
                                IAsyncResult ar =del3.BeginInvoke(pedido, this.url, "raiz", "", funcaoCallBack, null);

                                try
                                {
                                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerPai);
                                    delSequencer del = new delSequencer(bro.pedidoSequencer);
                                    funcaoCallBack = new AsyncCallback(OnExitSequencer);
                                    IAsyncResult ar1 =del.BeginInvoke(pedido,this.url, funcaoCallBack, null);
                                }catch(Exception e) { }
                            }
                            else    //Se este broker é já a raiz (se o publisher está na raiz, invocamos a função para obter o sequenciador aqui no proprio broker)
                                pedidoSequencer(pedido, this.url);

                            eventosEspera_pedido.Add(numeroPedido, evento);     //Adiciona o evento a um dictionary com o numero de pedido associado

                            listaEventosEspera.Remove(evento);

                            numeroPedido++;
                            listaPublishersLocais[evento.getNomePublisher()]++;
                            verificaFilaEsperaTotal();
                            break;
                        }
                    }
                }
            }
        }

        public void enviaSubscriberFloodingNO(Evento evento, Subscricao subscricao)
        {

            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
            if (topicos.Last().Equals("*"))
            {
                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                    EnviaSubscriberAssincrono(evento, subscricao);
            }
            else if (evento.getTopic().Equals(subscricao.getEvento().getTopic()))
                    EnviaSubscriberAssincrono(evento, subscricao);
        }

        public void enviaSubscriberFloodingFIFO(Evento evento, Subscricao subscricao)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
            if (topicos.Last().Equals("*"))
            {
                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                    EnviaSubscriberSincrono(evento, subscricao);
            }
            else if (subscricao.getEvento().getTopic().Equals(evento.getTopic())) //se der true, entao este subscritor está subscrito a este evento e recebe-o
                EnviaSubscriberSincrono(evento, subscricao);
        }

        public void enviaSubscriberFilteringNO(Evento evento,  Subscricao subscricao)
        {
          

            Evento novoEvento = new Evento(evento.getTopic());
            novoEvento.setContent(evento.getContent());
            novoEvento.setSeqNumberSite(evento.getSeqNumberSite());
            novoEvento.setUrlOrigem(evento.getUrlOrigem());

            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            //vamos percorrer a tabela de subscricoes e vamos ver quem sao os subscritores que subscrevem a este evento

            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
            if (topicos.Last().Equals("*"))
            {
                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                string[] topicosEventosRecebido = novoEvento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                {
                    ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());             

                    if (loggingLevel.Equals("full"))
                    {
                        IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                        pm.recebeBroLog(id, novoEvento.getNomePublisher(), novoEvento.getTopic(), novoEvento.getSeqNumber().ToString(), sub.getID());
                    }
                    try
                    {
                        if (tabelaEncaminhamento.ContainsKey(bro.getID())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                        {
                            novoEvento.setSite(bro.getSite());
                            lock(lockPropagacao)
                                listaEventosEsperaACKPropagacaoEvento.Add(novoEvento);

                            delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                            AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                            IAsyncResult ar =del1.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, "NO", funcaoCallBack, null);



                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[bro.getID()]);
                            delPropagacao del = new delPropagacao(broker.propagacaoEvento);
                            AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit9);
                            IAsyncResult ar1 =del.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, funcaoCallBack1, null);
                        }
                        else //caso contrario entregamos ao subscriber
                        {
                            EnviaSubscriberAssincrono(novoEvento, subscricao);
                        }
                    }
                    catch(Exception e) // se um broker falhar entretanto
                    {
                        lock(lockSentinela)
                        {
                            if (!sentinela4)
                            {
                                string urlBrokerSuspeito = sub.getUrlBroker();
                                sentinela4 = true;

                                string urlNovoBroker = sub.getBrokerEmEspera(urlBrokerSuspeito); //vamos buscar o broker irá ser o novo principal
                                IBroker novoBrokerPrincipal = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBroker);

                                lock(lock_)
                                    ajudaLock = true;

                                delRegistoBroker del1 = new delRegistoBroker(novoBrokerPrincipal.brokerDetetaSuspeito);
                                funcaoCallBack = new AsyncCallback(OnExit90);
                                IAsyncResult ar =del1.BeginInvoke(urlBrokerSuspeito, urlNovoBroker, this.url, funcaoCallBack, null);

                                //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 
                                if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                                {
                                    listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";
                                    listaBrokersPais[urlNovoBroker] = "ATIVO";

                                    tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                                }
                                // se isto se verificar então ia ser enviado para um broker filho
                                else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                                {
                                    BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));

                                    brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                    brokerSuspeito.setLider(urlNovoBroker); //atualizamos o lider

                                    tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                                }
                            }
                        }
                        try
                        {

                            lock (lock_)
                            {
                                if (ajudaLock)
                                    Monitor.Wait(lock_);
                            }

                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());
                            string urlAEnviar =tabelaEncaminhamento[broker.getIDOriginal()];

                            delVerificaPropragacao del2 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                            AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                            IAsyncResult ar =del2.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url,"NO", funcaoCallBack, null);

                            IBroker broker1 = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[broker.getIDOriginal()]);
                            delPropagacao del = new delPropagacao(broker1.propagacaoEvento);
                            AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit9);
                            IAsyncResult ar1 =del.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url, funcaoCallBack1, null);
                        }
                        catch (Exception d) { }
                    }
                }
            }
            else if (novoEvento.getTopic().Equals(subscricao.getEvento().getTopic()))
            {
                ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL()); //vamos buscar o subscriber para saber quem é o seu broker

                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());
                if (loggingLevel.Equals("full"))
                {
                    IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                    pm.recebeBroLog(id, novoEvento.getNomePublisher(), novoEvento.getTopic(), novoEvento.getSeqNumber().ToString(), sub.getID());
                }
                try
                {
                    if (tabelaEncaminhamento.ContainsKey(bro.getID())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                    {
                        novoEvento.setSite(bro.getSite());
                        lock(lockPropagacao)
                            listaEventosEsperaACKPropagacaoEvento.Add(novoEvento);

                        delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                        IAsyncResult ar =del1.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, "NO",funcaoCallBack, null);

                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[bro.getID()]);
                        delPropagacao del = new delPropagacao(broker.propagacaoEvento);
                        AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit9);
                        IAsyncResult ar1 =del.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, funcaoCallBack1, null);
                    }
                    else //se a tabela nao contiver este broker então podemos entregar diretamente ao subscriber. Isto verifica-se pois os brokers nao contêm a si proprios na sua tabela de encaminhamento
                    {
                        EnviaSubscriberAssincrono(novoEvento, subscricao);
                    }
                }
                catch (Exception e) //caso em que um broker falha
                {
                    lock(lockSentinela)
                    {
                        if (!sentinela4)
                        {
                            string urlBrokerSuspeito = sub.getUrlBroker();
                            sentinela4 = true;

                            string urlNovoBroker = sub.getBrokerEmEspera(urlBrokerSuspeito); //vamos buscar o broker irá ser o novo principal
                            IBroker novoBrokerPrincipal = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBroker);

                            lock(lock_)
                                 ajudaLock = true;
                            delRegistoBroker del1 = new delRegistoBroker(novoBrokerPrincipal.brokerDetetaSuspeito);
                            funcaoCallBack = new AsyncCallback(OnExit90);
                            IAsyncResult ar =del1.BeginInvoke(urlBrokerSuspeito, urlNovoBroker, this.url, funcaoCallBack, null);

                            //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 
                            if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                            {
                                listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";
                                listaBrokersPais[urlNovoBroker] = "ATIVO";

                                tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                            }
                            // se isto se verificar então ia ser enviado para um broker filho
                            else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                            {
                                BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));

                                brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                brokerSuspeito.setLider(urlNovoBroker); //atualizamos o lider

                                tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                            }
                        }
                    }
                    try
                    {
                        lock (lock_)
                        {
                            if (ajudaLock)
                                Monitor.Wait(lock_);
                        }

                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());
                        string urlAEnviar =tabelaEncaminhamento[broker.getIDOriginal()];

                        delVerificaPropragacao del2 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                        IAsyncResult ar =del2.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url,"NO", funcaoCallBack, null);

                        IBroker broker1 = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[broker.getIDOriginal()]);
                        delPropagacao del = new delPropagacao(broker1.propagacaoEvento);
                        AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit9);
                        IAsyncResult ar1 =del.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url,funcaoCallBack1, null);
                    }
                    catch (Exception d) { }
                }
            }
        }

        public void enviaSubscriberFilteringFIFO(Evento evento, Subscricao subscricao)
        {
            Evento novoEvento = new Evento(evento.getTopic());
            novoEvento.setContent(evento.getContent());
            novoEvento.setSeqNumberSite(evento.getSeqNumberSite());
            novoEvento.setUrlOrigem(evento.getUrlOrigem());

            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            //vamos percorrer a tabela de subscricoes e vamos ver quem sao os subscritores que subscrevem a este evento

            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
            if (topicos.Last().Equals("*"))
            {
                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                string[] topicosEventosRecebido = novoEvento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                {
                    ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());


                    if (loggingLevel.Equals("full"))
                    {
                        IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                        pm.recebeBroLog(id, novoEvento.getNomePublisher(), novoEvento.getTopic(), novoEvento.getSeqNumber().ToString(), sub.getID());
                    }
                    try
                    {
                        IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());

                        if (tabelaEncaminhamento.ContainsKey(bro.getID())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                        {
                            novoEvento.setSite(bro.getSite());
                            lock (lockPropagacao)
                                listaEventosEsperaACKPropagacaoEvento.Add(novoEvento);

                            delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                            AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                            IAsyncResult ar = del1.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, "FIFO", funcaoCallBack, null);

                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[bro.getID()]);

                            if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                            {
                                Site siteNovo = new Site(broker.getSite(), 0);
                                listaSites.Add(siteNovo);
                            }

                            Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));    //Vamos buscar o site para quem vamos mandar o evento                        

                            site.incrementSeqNumber();
                            novoEvento.setSeqNumberSite(site.getSeqNumber());


                            delPropagacao del = new delPropagacao(broker.propagacaoEventoFIFO);
                            funcaoCallBack = new AsyncCallback(OnExit);
                            IAsyncResult ar1 = del.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, funcaoCallBack, null);
                        }
                        else //caso contrario entregamos ao subscriber
                        {
                            sub.recebeEvento(novoEvento, true);
                        }
                        
                    }
                    catch (Exception e) // se um broker falhar entretanto
                    {
                        lock(lockSentinela)
                        {
                            if (!sentinela4)
                            {
                                string urlBrokerSuspeito = sub.getUrlBroker();
                                sentinela4 = true;

                                string urlNovoBroker = sub.getBrokerEmEspera(urlBrokerSuspeito); //vamos buscar o broker irá ser o novo principal
                                IBroker novoBrokerPrincipal = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBroker);

                                lock(lock_)
                                      ajudaLock = true;
                                delRegistoBroker del1 = new delRegistoBroker(novoBrokerPrincipal.brokerDetetaSuspeito);
                                funcaoCallBack = new AsyncCallback(OnExit90);
                                IAsyncResult ar =del1.BeginInvoke(urlBrokerSuspeito, urlNovoBroker, this.url, funcaoCallBack, null);

                                //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 
                                if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                                {
                                    listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";
                                    listaBrokersPais[urlNovoBroker] = "ATIVO";

                                    tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                                }
                                // se isto se verificar então ia ser enviado para um broker filho
                                else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                                {
                                    BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));

                                    brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                    brokerSuspeito.setLider(urlNovoBroker); //atualizamos o lider

                                    tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                                }
                            }
                        }
                        try
                        {
                            lock (lock_)
                            {
                                if (ajudaLock)
                                    Monitor.Wait(lock_);
                            }

                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());
                            string urlAEnviar =tabelaEncaminhamento[broker.getIDOriginal()];

                            delVerificaPropragacao del2 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                            AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                            IAsyncResult ar =del2.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url, "FIFO",funcaoCallBack, null);

                            IBroker broker1 = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[broker.getIDOriginal()]);
                            delPropagacao del = new delPropagacao(broker1.propagacaoEventoFIFO);
                            AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit9);
                            IAsyncResult ar1 =del.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url, funcaoCallBack1, null);
                        }
                        catch (Exception d) { }
                    }
                }
            }
            else if (novoEvento.getTopic().Equals(subscricao.getEvento().getTopic()))
            {

                ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL()); //vamos buscar o subscriber para saber quem é o seu broker

                if (loggingLevel.Equals("full"))
                {
                    IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                    pm.recebeBroLog(id, novoEvento.getNomePublisher(), novoEvento.getTopic(), novoEvento.getSeqNumber().ToString(), sub.getID());
                }

                try
                {
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());

                    if (tabelaEncaminhamento.ContainsKey(bro.getID())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                    {

                        novoEvento.setSite(bro.getSite());
                        lock (lockPropagacao)
                            listaEventosEsperaACKPropagacaoEvento.Add(novoEvento);

                        delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                        IAsyncResult ar = del1.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, "FIFO", funcaoCallBack, null);

                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[bro.getID()]);

                        if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                        {
                            Site siteNovo = new Site(broker.getSite(), 0);
                            listaSites.Add(siteNovo);
                        }

                        Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));    //Vamos buscar o site para quem vamos mandar o evento                        

                        site.incrementSeqNumber();
                        novoEvento.setSeqNumberSite(site.getSeqNumber());

                        delPropagacao del = new delPropagacao(broker.propagacaoEventoFIFO);
                        funcaoCallBack = new AsyncCallback(OnExit);
                        IAsyncResult ar2 = del.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, url, funcaoCallBack, null);
                    }
                    else //caso contrario entregamos ao subscriber
                    {

                        sub.recebeEvento(evento, true);
                    }
                }
                catch (Exception e) //caso em que um broker falha
                {
                    lock (lockSentinela)
                    {
                        if (!sentinela4)
                        {
                            string urlBrokerSuspeito = sub.getUrlBroker();
                            sentinela4 = true;

                            string urlNovoBroker = sub.getBrokerEmEspera(urlBrokerSuspeito); //vamos buscar o broker irá ser o novo principal
                            IBroker novoBrokerPrincipal = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBroker);

                            lock(lock_)
                                  ajudaLock = true;
                            delRegistoBroker del1 = new delRegistoBroker(novoBrokerPrincipal.brokerDetetaSuspeito);
                            funcaoCallBack = new AsyncCallback(OnExit90);
                            IAsyncResult ar =del1.BeginInvoke(urlBrokerSuspeito, urlNovoBroker, this.url, funcaoCallBack, null);

                            //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 
                            if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                            {
                                listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";
                                listaBrokersPais[urlNovoBroker] = "ATIVO";

                                tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                            }
                            // se isto se verificar então ia ser enviado para um broker filho
                            else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                            {
                                BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));

                                brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                brokerSuspeito.setLider(urlNovoBroker); //atualizamos o lider

                                tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                            }
                        }
                    }
                    try
                    {

                        lock (lock_)
                        {
                            if (ajudaLock)
                            {
                                Monitor.Wait(lock_);
                            }
                        }

                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());
                        string urlAEnviar =tabelaEncaminhamento[broker.getIDOriginal()];

                        delVerificaPropragacao del2 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                        IAsyncResult ar =del2.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url, "FIFO", funcaoCallBack, null);

                        IBroker broker1 = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[broker.getIDOriginal()]);
                        delPropagacao del = new delPropagacao(broker1.propagacaoEventoFIFO);
                        AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit9);
                        IAsyncResult ar1 =del.BeginInvoke(novoEvento, broker.getIDOriginal(), subscricao.getURL(), sub.getID(), subscricao, url, funcaoCallBack1, null);
                    }
                    catch (Exception d) { }
                }
            }
        }

        public void enviaSubscriberFilteringTOTAL(Evento evento, Subscricao subscricao, Pedido pedido)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            Evento novoEvento = new Evento(evento.getTopic());
            novoEvento.setContent(evento.getNomePublisher(), evento.getSeqNumber());
            novoEvento.setUrlOrigem(evento.getUrlOrigem());

            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
            if (topicos.Last().Equals("*"))
            {
                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                string[] topicosEventosRecebido = novoEvento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                {
                    ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());

                    novoEvento.setSequencer(pedido.getSubscritores()[sub.getID()]);     //Colocamos o sequencer no evento para o subscriber correspondente
                    novoEvento.setSubFinal(sub.getID());

                    if (loggingLevel.Equals("full"))
                    {
                        IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                        pm.recebeBroLog(id, novoEvento.getNomePublisher(), novoEvento.getTopic(), novoEvento.getSeqNumber().ToString(), sub.getID());
                    }
                    if (tabelaEncaminhamento.ContainsKey(bro.getID())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                    {
                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[bro.getID()]);

                        if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                        {
                            Site siteNovo = new Site(broker.getSite(), 0);
                            listaSites.Add(siteNovo);
                        }
                        Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));    //Vamos buscar o site para quem vamos mandar o evento                        
                        site.incrementSeqNumber();
                        novoEvento.setSeqNumberSite(site.getSeqNumber());       //Colocamos o novo SeqNumber correspondente a esse site

                        lock (lockTotal)
                               listaEsperaPropagacaoTotalACK.Add(evento);

                        delPropagacaoFIFO del2 = new delPropagacaoFIFO(verificaPropagacaoTotalACK);
                        funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                        IAsyncResult ar =del2.BeginInvoke(evento, evento.getIdBrokerFinal(), evento.getUrlSubscriber(), evento.getIdSubscriber(), evento.getSubscricao(), this.url, funcaoCallBack, null);

                        try
                        {
                            delPropagacaoFIFO del = new delPropagacaoFIFO(broker.propagacaoEventoTOTAL);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar1 =del.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, getUrl(), funcaoCallBack, null);
                        }catch(Exception e) { }
                    }
                    else //se a tabela nao contiver este broker então podemos entregar diretamente ao subscriber. Isto verifica-se pois os brokers nao contêm a si proprios na sua tabela de encaminhamento
                    {
                        lock(lock_4)
                            listaOrganizada.Add(novoEvento);
                        verificaEntregaTOTAL();
                    }
                }
            }
            else if (novoEvento.getTopic().Equals(subscricao.getEvento().getTopic()))
            {
                ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL()); //vamos buscar o subscriber para saber quem é o seu broker
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());

                novoEvento.setSequencer(pedido.getSubscritores()[sub.getID()]);     //Colocamos o sequencer no evento para o subscriber correspondente
                novoEvento.setSubFinal(sub.getID());

                if (loggingLevel.Equals("full"))
                {
                    IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                    pm.recebeBroLog(id, novoEvento.getNomePublisher(), novoEvento.getTopic(), novoEvento.getSeqNumber().ToString(), sub.getID());
                }
                try
                {
                    if (tabelaEncaminhamento.ContainsKey(bro.getID())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                    {
                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[bro.getID()]);

                        if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                        {
                            Site siteNovo = new Site(broker.getSite(), 0);
                            listaSites.Add(siteNovo);
                        }
                        Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));    //Vamos buscar o site para quem vamos mandar o evento                        
                        site.incrementSeqNumber();
                        novoEvento.setSeqNumberSite(site.getSeqNumber());       //Colocamos o novo SeqNumber correspondente a esse site

                        lock(lockTotal)
                            listaEsperaPropagacaoTotalACK.Add(novoEvento);

                        delPropagacaoFIFO del = new delPropagacaoFIFO(verificaPropagacaoTotalACK);
                        funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                        IAsyncResult ar =del.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, this.url, funcaoCallBack, null);

                        try
                        {

                            delPropagacaoFIFO del2 = new delPropagacaoFIFO(broker.propagacaoEventoTOTAL);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar1 =del2.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, this.url, funcaoCallBack, null);
                        }catch(Exception e) { }
                    }
                    else //se a tabela nao contiver este broker então podemos entregar diretamente ao subscriber. Isto verifica-se pois os brokers nao contêm a si proprios na sua tabela de encaminhamento
                    {
                        lock(lock_4)
                            listaOrganizada.Add(novoEvento);
                        verificaEntregaTOTAL();
                    }
                }catch(Exception e) // se houver um crash entretanto
                {
                    lock(lockSentinela)
                    {
                        if(!sentinela4)
                        {
                            sentinela4 = true;

                            string urlBrokerSuspeito = sub.getUrlBroker();
                            string urlNovoBroker = sub.getBrokerEmEspera(urlBrokerSuspeito); //vamos buscar o broker irá ser o novo principal
                            IBroker novoBrokerPrincipal = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBroker);

                            lock (lock_)
                                    ajudaLock = true;

                            delRegistoBroker del1 = new delRegistoBroker(novoBrokerPrincipal.brokerDetetaSuspeito);
                            funcaoCallBack = new AsyncCallback(OnExit90);
                            IAsyncResult ar =del1.BeginInvoke(urlBrokerSuspeito, urlNovoBroker, this.url, funcaoCallBack, null);

                            //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 
                            if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                            {
                                listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";
                                listaBrokersPais[urlNovoBroker] = "ATIVO";

                                tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                            }
                            // se isto se verificar então ia ser enviado para um broker filho
                            else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                            {
                                BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));

                                brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                brokerSuspeito.setLider(urlNovoBroker); //atualizamos o lider

                                tabelaEncaminhamento[novoBrokerPrincipal.getIDOriginal()] = urlNovoBroker; //atualizamos a tabela de encaminhamento
                            }
                        }
                        try
                        {
                            lock (lock_)
                            {
                                if (ajudaLock)
                                    Monitor.Wait(lock_);
                            }

                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), sub.getUrlBroker());
                            string urlAEnviar =tabelaEncaminhamento[broker.getIDOriginal()];

                            delPropagacaoFIFO del = new delPropagacaoFIFO(verificaPropagacaoTotalACK);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar =del.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, this.url, funcaoCallBack, null);

                            IBroker broker1 = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[broker.getIDOriginal()]);
                            delPropagacaoFIFO del2 = new delPropagacaoFIFO(broker.propagacaoEventoTOTAL);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar1 =del2.BeginInvoke(novoEvento, bro.getID(), subscricao.getURL(), sub.getID(), subscricao, this.url, funcaoCallBack, null);
                        }
                        catch (Exception d) { }
                    }
                }
            }
        }

        public void verificaPropagacaoTotalACK(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou)
        {
            if (freezeAtivo)
            {
                Monitor.Wait(this);
                if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                    return;
                else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                {
                    KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                    IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                    delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                    funcaoCallBack = new AsyncCallback(OnExit4);
                    IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                    return;
                }
             }

            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           
            lock(lockTotal)
            {
                try
                {
                    if (listaEsperaPropagacaoTotalACK.Any(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(evento.getSite()) && x.getNomePublisher().Equals(evento.getNomePublisher())))
                    {
                        lock (lockSentinela)
                        {
                            if (!sentinela)
                            {
                                sentinela = true;

                                //vamos ver se é um broker pai ou filho, porque no filtering nao há a distinção
                                //se vamos enviar para um pai ou filho, por isso precisamos de saber para qual vamos 
                                //enviar para ir buscar o seu irmao
                                string urlBrokerSuspeito = tabelaEncaminhamento[idBrokerDestinoFinal];

                                //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 
                                if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                                {
                                    listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";

                                    KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                    listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";

                                    tabelaEncaminhamento[idBrokerDestinoFinal] = urlNovoBrokerAtivo.Key; //atualizamos a tabela de encaminhamento

                                    try
                                    {
                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                        delRegistoBroker del1 = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                        funcaoCallBack = new AsyncCallback(OnExit3);
                                        IAsyncResult ar1 =del1.BeginInvoke(urlBrokerSuspeito, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                    }
                                    catch (Exception e) { }

                                }// se isto se verificar então ia ser enviado para um broker filho
                                else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                                {
                                    BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));

                                    brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                    string urlNovoPrincipal = brokerSuspeito.novoLider(urlBrokerSuspeito); //é atualizado o novo broker principal e é recebido o seu url

                                    tabelaEncaminhamento[idBrokerDestinoFinal] = urlNovoPrincipal; //atualizamos a tabela de encaminhamento

                                    try
                                    {
                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoPrincipal);
                                        delRegistoBroker del1 = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                        funcaoCallBack = new AsyncCallback(OnExit3);
                                        IAsyncResult ar2 =del1.BeginInvoke(urlBrokerSuspeito, urlNovoPrincipal, this.url, funcaoCallBack, null);
                                    }
                                    catch (Exception e) { }
                                }
                            }

                            delPropagacaoFIFO del2 = new delPropagacaoFIFO(reenviaPropagacaoTotal);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar =del2.BeginInvoke(evento, idBrokerDestinoFinal, urlSubscriber, idSubscriber, subscricao, urlQuemEnviou, funcaoCallBack, null);
                        }
                    }
                }catch(Exception e) { }
            }
        }

        public void reenviaPropagacaoTotal(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idBrokerDestinoFinal]);
            if (!urlQuemEnviou.Equals(broker.getUrl()))
            {
                try
                {
                    delPropagacaoFIFO del2 = new delPropagacaoFIFO(broker.propagacaoEventoTOTAL);
                    funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                    IAsyncResult ar =del2.BeginInvoke(evento, broker.getID(), subscricao.getURL(), idSubscriber, subscricao, urlQuemEnviou, funcaoCallBack, null);
                }
                catch(Exception e) { }
            }
            else
                lock(lockTotal)
                    listaEsperaPropagacaoTotalACK.RemoveAll(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(site) && x.getNomePublisher().Equals(evento.getNomePublisher()));

            Thread.Sleep(10000);
            
            lock(lockTotal)
            {
                if (listaEsperaPropagacaoTotalACK.Any(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(evento.getSite()) && x.getNomePublisher().Equals(evento.getNomePublisher())))
                {
                    lock (lockSentinela)
                    {
                        if(!sentinela2)
                        {
                            sentinela = false;
                            sentinela2 = true;
                        }
                    }
                    delPropagacaoFIFO del = new delPropagacaoFIFO(verificaPropagacaoTotalACK);
                    funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                    IAsyncResult ar =del.BeginInvoke(evento, idBrokerDestinoFinal, subscricao.getURL(), idSubscriber, subscricao, urlQuemEnviou, funcaoCallBack, null);

                }
                else
                {
                    lock (lockSentinela)
                    sentinela2 = false;
                }
            }

        }

        public void verificaListaEsperaPropagacaoTOTAL()
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_4)
            {
                foreach (Evento evento in listaPropagacaoEspera)
                {
                    IBroker brokerQueEnviou = (IBroker)Activator.GetObject(typeof(IBroker), evento.getUrlBrokerEnviou());
                    Site siteBrokerEnviou = listaSites2.Find(x => x.getSite().Equals(brokerQueEnviou.getSite()));

                    if (evento.getSeqNumberSite() == siteBrokerEnviou.getSeqNumber() + 1)   //Se for o evento que estávamos à espera
                    {
                        if (tabelaEncaminhamento.ContainsKey(evento.getIdBrokerFinal())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                        {
                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[evento.getIdBrokerFinal()]);

                            if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                            {
                                Site sitenovo = new Site(broker.getSite(), 0);
                                listaSites.Add(sitenovo);
                            }

                            Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));

                            site.incrementSeqNumber();
                            siteBrokerEnviou.incrementSeqNumber();
                            evento.setSeqNumberSite(site.getSeqNumber());
                            listaPropagacaoEspera.Remove(evento);

                            lock (lockTotal)
                                listaEsperaPropagacaoTotalACK.Add(evento);

                            delPropagacaoFIFO del2 = new delPropagacaoFIFO(verificaPropagacaoTotalACK);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar =del2.BeginInvoke(evento, evento.getIdBrokerFinal(), evento.getUrlSubscriber(), evento.getIdSubscriber(), evento.getSubscricao(), this.url, funcaoCallBack, null);

                            try
                            {
                                delPropagacaoFIFO del = new delPropagacaoFIFO(broker.propagacaoEventoTOTAL);
                                funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                                IAsyncResult ar1 =del.BeginInvoke(evento, evento.getIdBrokerFinal(), evento.getUrlSubscriber(), evento.getIdSubscriber(), evento.getSubscricao(), this.url, funcaoCallBack, null);
                            }catch(Exception e) { }

                            verificaListaEsperaPropagacaoTOTAL();
                            break;
                        }
                        else        //Se já estamos no broker que tem o subscriber correspondente
                        {
                            listaPropagacaoEspera.Remove(evento);
                            siteBrokerEnviou.incrementSeqNumber();
                            lock(lock_4)
                                listaOrganizada.Add(evento);
                            verificaEntregaTOTAL();
                            verificaListaEsperaPropagacaoTOTAL();
                            break;
                        }
                    }
                }
            }
        }

        public void verificaEntregaTOTAL()
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_4)
            {
                foreach (Evento evento in listaOrganizada)
                {
                    string urlSubFinal = tabelaSubscricoes.Find(x => x.getIDSubscriber().Equals(evento.getSubFinal())).getURL();

                    ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), urlSubFinal);

                    if (evento.getSequencer() == sub.getSequencer() + 1)
                    {
                        sub.incrementSequencer();
                        sub.recebeEvento(evento, true);
                        lock(lock_4)
                            listaOrganizada.Remove(evento);

                        verificaEntregaTOTAL();
                        break;
                    }
                }
            }
        }

        public void propagacaoEvento(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou)
        {
            string siteParaACK = evento.getSite();
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            if (loggingLevel.Equals("full"))
            {
                IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), idSubscriber);
            }

            if (tabelaEncaminhamento.ContainsKey(idBrokerDestinoFinal)) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
            {

                //evento.setSite(site);
                lock (lockPropagacao)
                    listaEventosEsperaACKPropagacaoEvento.Add(evento);

                delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                IAsyncResult ar = del1.BeginInvoke(evento, idBrokerDestinoFinal, subscricao.getURL(), idSubscriber, subscricao, url, "NO", funcaoCallBack, null);

                try
                {
                    IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idBrokerDestinoFinal]);
                    delPropagacao del = new delPropagacao(broker.propagacaoEvento);
                    AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit9);
                    IAsyncResult ar1 = del.BeginInvoke(evento, idBrokerDestinoFinal, urlSubscriber, idSubscriber, subscricao, url, funcaoCallBack1, null);
                }
                catch (Exception e) { }
            }
            else //se a tabela nao contiver este broker então podemos entregar diretamente ao subscriber. Isto verifica-se pois os brokers nao contêm a si proprios na sua tabela de encaminhamento
            {
                EnviaSubscriberAssincrono(evento, urlSubscriber);
            }

            try
            {
                //enviamos o ACK para o broker que enviou o evento
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviou);
                delACK2 del = new delACK2(bro.recebeAckPropagacao);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), siteParaACK, evento.getNomePublisher(), funcaoCallBack, null);
            }
            catch (Exception e) { }

        }

        public void propagacaoEventoFIFO(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou)
        {
            string siteParaACK = evento.getSite();
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            if (loggingLevel.Equals("full"))
            {
                IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), idSubscriber);
            }

            lock(lock_4)
            {

                IBroker brokerQueEnviou = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviou);

                if (!listaSites2.Exists(x => x.getSite().Equals(brokerQueEnviou.getSite()))) //Se for a primeira vez que estamos a receber deste site
                {
                    Site site = new Site(brokerQueEnviou.getSite(), 0);
                    listaSites2.Add(site);
                }

                Site siteBrokerEnviou = listaSites2.Find(x => x.getSite().Equals(brokerQueEnviou.getSite()));

                if (evento.getSeqNumberSite() == siteBrokerEnviou.getSeqNumber() + 1)   //Se for o evento que estávamos à espera
                {

                    if (tabelaEncaminhamento.ContainsKey(idBrokerDestinoFinal)) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                    {
                        //evento.setSite(site);
                        lock (lockPropagacao)
                            listaEventosEsperaACKPropagacaoEvento.Add(evento);

                        delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                        IAsyncResult ar =del1.BeginInvoke(evento, idBrokerDestinoFinal, subscricao.getURL(), idSubscriber, subscricao, url, "FIFO", funcaoCallBack, null);

                        try
                        {
                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idBrokerDestinoFinal]);

                            if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                            {
                                Site sitenovo = new Site(broker.getSite(), 0);
                                listaSites.Add(sitenovo);
                            }

                            Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));

                            site.incrementSeqNumber();
                            siteBrokerEnviou.incrementSeqNumber();
                            evento.setSeqNumberSite(site.getSeqNumber());

                            delPropagacaoFIFO del = new delPropagacaoFIFO(broker.propagacaoEventoFIFO);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar1 =del.BeginInvoke(evento, idBrokerDestinoFinal, urlSubscriber, idSubscriber, subscricao, getUrl(), funcaoCallBack, null);

                            verificaListaEsperaPropagacao();
                        }
                        catch (Exception e) { }
                    }
                    else//se a tabela nao contiver este broker então podemos entregar diretamente ao subscriber. Isto verifica-se pois os brokers nao contêm a si proprios na sua tabela de encaminhamento
                    {
                        ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());
                        sub.recebeEvento(evento, true);
                        siteBrokerEnviou.incrementSeqNumber();
                        verificaListaEsperaPropagacao();
                    }
                }
                else 
                {
                    
                    evento.setIdBrokerFinal(idBrokerDestinoFinal);
                    evento.setUrlSubscriber(urlSubscriber);
                    evento.setIdSubscriber(idSubscriber);
                    evento.setUrlBrokerEnviou(urlQuemEnviou);
                    evento.setSubscricao(subscricao);
                    lock (lock_4)
                        listaPropagacaoEspera.Add(evento);
                }
                try
                {
                //enviamos o ACK para o broker que enviou o evento
                    IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviou);
                    delACK2 del = new delACK2(bro.recebeAckPropagacao);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), siteParaACK, evento.getNomePublisher(), funcaoCallBack, null);
                }
                catch (Exception e) { }
            }
        }

        public void propagacaoEventoTOTAL(Evento evento, string idBrokerDestinoFinal, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou)
        {
            string siteParaACK = evento.getSite();

            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            if (loggingLevel.Equals("full"))
            {
                IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), idSubscriber);
            }

            lock (lock_4)
            {
                IBroker brokerQueEnviou = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviou);

                if (!listaSites2.Exists(x => x.getSite().Equals(brokerQueEnviou.getSite()))) //Se for a primeira vez que estamos a receber deste site
                {
                    Site site = new Site(brokerQueEnviou.getSite(), 0);
                    listaSites2.Add(site);
                }

                Site siteBrokerEnviou = listaSites2.Find(x => x.getSite().Equals(brokerQueEnviou.getSite()));

                if (evento.getSeqNumberSite() == siteBrokerEnviou.getSeqNumber() + 1)   //Se for o evento que estávamos à espera
                {
                    if (tabelaEncaminhamento.ContainsKey(idBrokerDestinoFinal)) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                    {
                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idBrokerDestinoFinal]);

                        if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                        {
                            Site sitenovo = new Site(broker.getSite(), 0);
                            listaSites.Add(sitenovo);
                        }

                        Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));

                        site.incrementSeqNumber();
                        siteBrokerEnviou.incrementSeqNumber();
                        evento.setSeqNumberSite(site.getSeqNumber());

                        lock (lockTotal)
                               listaEsperaPropagacaoTotalACK.Add(evento);

                        delPropagacaoFIFO del2 = new delPropagacaoFIFO(verificaPropagacaoTotalACK);
                        funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                        IAsyncResult ar =del2.BeginInvoke(evento, evento.getIdBrokerFinal(), evento.getUrlSubscriber(), evento.getIdSubscriber(), evento.getSubscricao(), this.url, funcaoCallBack, null);

                        try
                        {
                            delPropagacaoFIFO del = new delPropagacaoFIFO(broker.propagacaoEventoTOTAL);
                            funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                            IAsyncResult ar1 =del.BeginInvoke(evento, idBrokerDestinoFinal, urlSubscriber, idSubscriber, subscricao, getUrl(), funcaoCallBack, null);
                        }catch(Exception e) { }

                        verificaListaEsperaPropagacaoTOTAL();
                    }
                    else        //Se já estamos no broker que tem o subscriber
                    {
                        siteBrokerEnviou.incrementSeqNumber();
                        lock(lock_4)
                            listaOrganizada.Add(evento);
                        verificaEntregaTOTAL();
                        verificaListaEsperaPropagacaoTOTAL();
                    }
                }
                else
                {
                    evento.setIdBrokerFinal(idBrokerDestinoFinal);
                    evento.setUrlSubscriber(urlSubscriber);
                    evento.setIdSubscriber(idSubscriber);
                    evento.setUrlBrokerEnviou(urlQuemEnviou);
                    evento.setSubscricao(subscricao);

                    listaPropagacaoEspera.Add(evento);
                }
            }

            try
            {
                //enviamos o ACK para o broker que enviou o evento
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlQuemEnviou);
                delACK2 del = new delACK2(bro.recebeACKPropagacaoTotal);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(evento.getSeqNumber(), siteParaACK, evento.getNomePublisher(), funcaoCallBack, null);
            }
            catch (Exception e) { }
        }

        public void verificaEntregaCompletaPropagacao(Evento evento, string idBrokerDestino, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou, string tipoOrdenacao)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;                  
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           
            lock (lockPropagacao)
            {
                if (listaEventosEsperaACKPropagacaoEvento.Any(x => x.getSeqNumber() == evento.getSeqNumber() &&  x.getSite().Equals(evento.getSite()) && x.getNomePublisher().Equals(evento.getNomePublisher())))
                {

                    lock (lockSentinela)
                    {
                        if (!sentinela)
                        {
                            sentinela = true;

                            //vamos ver se é um broker pai ou filho, porque no filtering nao há a distinção
                            //se vamos enviar para um pai ou filho, por isso precisamos de saber para qual vamos 
                            //enviar para ir buscar o seu irmao
                            string urlBrokerSuspeito = tabelaEncaminhamento[idBrokerDestino];

                            //se isto se verificar então ia ser enviado para um broker pai e vamos buscar o seu irmao 
                            if (listaBrokersPais.Any(x => x.Key.Equals(urlBrokerSuspeito)))
                            {
                                listaBrokersPais[urlBrokerSuspeito] = "SUSPEITO";

                                KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";

                                tabelaEncaminhamento[idBrokerDestino] = urlNovoBrokerAtivo.Key; //atualizamos a tabela de encaminhamento

                                try
                                {
                                    IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                    delRegistoBroker del1 = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                    funcaoCallBack = new AsyncCallback(OnExit3);
                                    IAsyncResult ar1 =del1.BeginInvoke(urlBrokerSuspeito, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                }
                                catch (Exception e) { }

                            }
                            // se isto se verificar então ia ser enviado para um broker filho
                            else if (listaFilhos.Any(x => x.getBroker().Equals(urlBrokerSuspeito)))
                            {
                                BrokerFilho brokerSuspeito = listaFilhos.Single(x => x.getBroker().Equals(urlBrokerSuspeito));

                                brokerSuspeito.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito
                                string urlNovoPrincipal = brokerSuspeito.novoLider(urlBrokerSuspeito); //é atualizado o novo broker principal e é recebido o seu url

                                tabelaEncaminhamento[idBrokerDestino] = urlNovoPrincipal; //atualizamos a tabela de encaminhamento

                                try
                                {
                                    IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoPrincipal);
                                    delRegistoBroker del1 = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                    funcaoCallBack = new AsyncCallback(OnExit3);
                                    IAsyncResult ar1 =del1.BeginInvoke(urlBrokerSuspeito, urlNovoPrincipal, this.url, funcaoCallBack, null);
                                }
                                catch (Exception e) { }
                            }

                        }
                        //reenviamos
                        delVerificaPropragacao del = new delVerificaPropragacao(reenviaPropagacao);
                        AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit13);
                        IAsyncResult ar =del.BeginInvoke(evento, idBrokerDestino, urlSubscriber, idSubscriber, subscricao, urlQuemEnviou, tipoOrdenacao, funcaoCallBack1, null);
                    }
                }
                else
                {
                }
            }
        }

        public void reenviaPropagacao(Evento evento, string idBrokerDestino, string urlSubscriber, string idSubscriber, Subscricao subscricao, string urlQuemEnviou, string tipoOrdenacao)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[idBrokerDestino]);
            if (!urlQuemEnviou.Equals(broker.getUrl()))
            {
                if (tipoOrdenacao.Equals("NO"))
                {
                    delPropagacao del = new delPropagacao(broker.propagacaoEvento);
                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit9);
                    IAsyncResult ar =del.BeginInvoke(evento, idBrokerDestino, subscricao.getURL(), idSubscriber, subscricao, urlQuemEnviou, funcaoCallBack, null);
                }
                else if(tipoOrdenacao.Equals("FIFO"))
                {
                    delPropagacao del = new delPropagacao(broker.propagacaoEventoFIFO);
                    AsyncCallback funcaoCallBack = new AsyncCallback(OnExit9);
                    IAsyncResult ar =del.BeginInvoke(evento, idBrokerDestino, subscricao.getURL(), idSubscriber, subscricao, urlQuemEnviou, funcaoCallBack, null);
                }
            }
            else
            {
                lock(lockPropagacao)
                    listaEventosEsperaACKPropagacaoEvento.RemoveAll(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(site) && x.getNomePublisher().Equals(evento.getNomePublisher()));
            }
            Thread.Sleep(10000);

            lock(lockPropagacao)
            {
                if (listaEventosEsperaACKPropagacaoEvento.Any(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(evento.getSite()) && x.getNomePublisher().Equals(evento.getNomePublisher())))
                {
                    lock (lockSentinela)
                    {
                        if (!sentinela2)
                        {
                            sentinela = false;
                            sentinela2 = true;
                        }
                    }

                    if (tipoOrdenacao.Equals("NO"))
                    {
                        delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                        AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit13);
                        IAsyncResult ar =del1.BeginInvoke(evento, idBrokerDestino, subscricao.getURL(), idSubscriber, subscricao, urlQuemEnviou, "NO", funcaoCallBack1, null);
                    }
                    else if (tipoOrdenacao.Equals("FIFO"))
                    {
                        delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                        AsyncCallback funcaoCallBack1 = new AsyncCallback(OnExit13);
                        IAsyncResult ar =del1.BeginInvoke(evento, idBrokerDestino, subscricao.getURL(), idSubscriber, subscricao, urlQuemEnviou, "FIFO", funcaoCallBack1, null);
                    }
                }
                else
                {
                    lock (lockSentinela)
                    sentinela2 = false;
                    
                }
            }
        }

        public void enviaBroker(Evento evento, string urlAEnviar, string nodeAEnviar, string site, string urlQuemEnviou)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar1 =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            if (loggingLevel.Equals("full"))
            {
                IPuppet pm = (IPuppet)Activator.GetObject(typeof(IPuppet), urlPuppet);
                pm.recebeBroLog(id, evento.getNomePublisher(), evento.getTopic(), evento.getSeqNumber().ToString(), "");
            }
            Evento novoEvento = new Evento(evento.getTopic());
            novoEvento.setContent(evento.getContent());
            novoEvento.setSeqNumberSite(evento.getSeqNumberSite());
            novoEvento.setUrlOrigem(evento.getUrlOrigem());
            novoEvento.setQuemEnviouEvento("broker");
            novoEvento.setSite(site);

            listaEventosEsperaACK.Add(novoEvento); //adicionamos à lista de esperar para posteriormente verificarmos se foi entregue com sucesso ou nao
            lock (lock_)
            {
                if (ajudaLock)
                    Monitor.Wait(lock_);
            }

            delFault del1 = new delFault(verificaEntregaCompleta);
            funcaoCallBack = new AsyncCallback(OnExit3);
            IAsyncResult ar =del1.BeginInvoke(novoEvento, nodeAEnviar,site, urlQuemEnviou, funcaoCallBack, null);

            try
            {
                IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), urlAEnviar);
                delTrata del = new delTrata(bro.recebeEvento);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar1 =del.BeginInvoke(novoEvento, false, url, funcaoCallBack, null); //enviamos o seu URL para o pai saber que nao tem que voltar a enviar para este nó
            }catch(Exception e) { }
        }

        //função é responsável por ver se é necessário reenviar o evento ou nao
        public void verificaEntregaCompleta(Evento evento, string nodeAEnviar, string site, string urlQuemEnviou)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           
            lock (lockEspera)
            {
                if (listaEventosEsperaACK.Any(x =>x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(evento.getSite()) &&  x.getNomePublisher().Equals(evento.getNomePublisher())))
                {
                    if (nodeAEnviar.Equals("pai"))
                    {
                        lock(lockSentinela)
                        {
                            if (!sentinela3)
                            {
                                sentinela3 = true;
                                
                                    KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                                    listaBrokersPais[urlBrokerAtivo.Key] = "SUSPEITO";

                                    KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                    listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";
                                    urlBrokerPai = urlNovoBrokerAtivo.Key;
                                try
                                {
                                    IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                    delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                    funcaoCallBack = new AsyncCallback(OnExit3);
                                    IAsyncResult ar1 =del.BeginInvoke(urlBrokerAtivo.Key, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                }
                                catch (Exception e) { }
                            }
                        }

                        
                        delFault del1 = new delFault(reenviaBro);
                        funcaoCallBack = new AsyncCallback(OnExit3);
                        IAsyncResult ar =del1.BeginInvoke(evento, nodeAEnviar, site, urlQuemEnviou, funcaoCallBack, null);
                        
                    }
                    else if (nodeAEnviar.Equals("filho"))
                    {
                        lock(lockSentinela)
                        {
                            if (!sentinela)
                            {
                                sentinela = true;

                                BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));
                                string urlBrokerSuspeito = broker.getBroker();
                                broker.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito

                                string urlNovoPrincipal = broker.novoLider(urlBrokerSuspeito); //é atualizado o novo broker principal e é recebido o seu url                          

                                try
                                {
                                    IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoPrincipal);
                                    delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                    funcaoCallBack = new AsyncCallback(OnExit3);
                                    IAsyncResult ar1 =del.BeginInvoke(urlBrokerSuspeito, urlNovoPrincipal, this.url, funcaoCallBack, null);
                                }
                                catch (Exception e) { }
                            }
                        }

                        delFault del1 = new delFault(reenviaBro);
                        funcaoCallBack = new AsyncCallback(OnExit3);
                        IAsyncResult ar =del1.BeginInvoke(evento, nodeAEnviar, site, urlQuemEnviou, funcaoCallBack, null);
                    }
                }
                else
                {
                }
            }
        }

        public void reenviaBro(Evento evento, string nodeAEnviar, string site, string urlQuemEnviou)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            IBroker objetoBroker;

            lock (lock_)
            {
                if (ajudaLock)
                    Monitor.Wait(lock_);
            }
            try
            {
                if (nodeAEnviar.Equals("filho"))
                {
                    BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));
                    if (!urlQuemEnviou.Equals(broker.getBroker()))
                    {
                        objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), broker.getBroker());

                        delTrata del = new delTrata(objetoBroker.recebeEvento);
                        funcaoCallBack = new AsyncCallback(OnExit);
                        IAsyncResult ar =del.BeginInvoke(evento, false, url, funcaoCallBack, null);
                    }
                    else
                        lock(lockEspera)
                            listaEventosEsperaACK.RemoveAll(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(site) && x.getNomePublisher().Equals(evento.getNomePublisher()));

                }
                else if (nodeAEnviar.Equals("pai"))
                {
                    KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                    if (!urlBrokerAtivo.Key.Equals(urlQuemEnviou))
                    {
                        objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);
                        delTrata del = new delTrata(objetoBroker.recebeEvento);
                        funcaoCallBack = new AsyncCallback(OnExit);
                        IAsyncResult ar =del.BeginInvoke(evento, false, url, funcaoCallBack, null);
                    }
                    else
                        lock(lockEspera)
                            listaEventosEsperaACK.RemoveAll(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(site) && x.getNomePublisher().Equals(evento.getNomePublisher()));

                }
            }
            catch (Exception e) { }
           
            Thread.Sleep(10000);
            lock (lockEspera)
            {
                if (listaEventosEsperaACK.Any(x => x.getSeqNumber() == evento.getSeqNumber() && x.getSite().Equals(evento.getSite()) && x.getNomePublisher().Equals(evento.getNomePublisher())))
                {
                    lock (lockSentinela)
                    {
                        if (!sentinela2)
                        {
                            sentinela = false;
                            sentinela3 = false;
                            sentinela2 = true;
                        }
                    }
                    delFault del1 = new delFault(verificaEntregaCompleta);
                    funcaoCallBack = new AsyncCallback(OnExit3);
                    IAsyncResult ar =del1.BeginInvoke(evento, nodeAEnviar, site,urlQuemEnviou, funcaoCallBack, null);
                }
                else
                {
                    lock(lockSentinela)
                        sentinela2 = false;
                }
            }
        }

        //verificaEntregaCompleta para verificação se o registoSubscriber foi enviado corretamente
        public void verificaEntregaCompletaRegistoSubscriber(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento, string nodeAEnviar, string funcaoAInvocar, string site)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            
            Thread.Sleep(10000); //se passar X tempo e o evento nao tiver sido removido da lista, ou seja nao recebeu um ACK, entao reenviamos           
            lock(lockRegistoSubscriber)
            {
                if (funcaoAInvocar.Equals("subscribe"))
                {
                    try
                    {

                        if (listaEventosEsperaACKRegistoSubscriber.Any(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber())))
                        {

                            if (nodeAEnviar.Equals("pai"))
                            {
                                lock (lockSentinela)
                                {
                                    if (!sentinela3)
                                    {
                                        sentinela3 = true;
                                        KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                                        listaBrokersPais[urlBrokerAtivo.Key] = "SUSPEITO";

                                        KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                        listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";

                                        urlBrokerPai = urlNovoBrokerAtivo.Key;

                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                        try
                                        {

                                            delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                            funcaoCallBack = new AsyncCallback(OnExit3);
                                            IAsyncResult ar2 = del.BeginInvoke(urlBrokerAtivo.Key, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                        }
                                        catch (Exception e) { }
                                    }
                                }

                                delRegistoSubscriber3 del1 = new delRegistoSubscriber3(reenviaRegistosubscricao);
                                funcaoCallBack = new AsyncCallback(OnExit10);
                                IAsyncResult ar = del1.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, nodeAEnviar, funcaoAInvocar, site, funcaoCallBack, null);

                            }
                            else if (nodeAEnviar.Equals("filho"))
                            {
                                lock (lockSentinela)
                                {
                                    if (!sentinela)
                                    {
                                        sentinela = true;

                                        BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));

                                        string urlBrokerSuspeito = broker.getBroker();
                                        broker.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito

                                        string urlNovoPrincipal = broker.novoLider(urlBrokerSuspeito); //é atualizado o novo broker principal e é recebido o seu url

                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoPrincipal);
                                        try
                                        {
                                            delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                            funcaoCallBack = new AsyncCallback(OnExit3);
                                            IAsyncResult ar1 = del.BeginInvoke(urlBrokerSuspeito, urlNovoPrincipal, this.url, funcaoCallBack, null);
                                        }
                                        catch (Exception e) { }
                                    }
                                }

                                delRegistoSubscriber3 del1 = new delRegistoSubscriber3(reenviaRegistosubscricao);
                                funcaoCallBack = new AsyncCallback(OnExit10);
                                IAsyncResult ar = del1.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, nodeAEnviar, funcaoAInvocar, site, funcaoCallBack, null);
                            }
                        }
                    }catch(Exception e) {  }
                }
                else if (funcaoAInvocar.Equals("unsubscribe"))
                {
                    try
                    {
                        if (listaEventosEsperaACKRegistoSubscriber2.Any(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber())))
                        {
                            if (nodeAEnviar.Equals("pai"))
                            {
                                lock (lockSentinela)
                                {
                                    if (!sentinela3)
                                    {
                                        sentinela3 = true;
                                        KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                                        listaBrokersPais[urlBrokerAtivo.Key] = "SUSPEITO";

                                        KeyValuePair<string, string> urlNovoBrokerAtivo = listaBrokersPais.First(x => x.Value.Equals("ESPERA")); //vamos buscar o primeiro broker que está à espera e tornamo-lo ativo
                                        listaBrokersPais[urlNovoBrokerAtivo.Key] = "ATIVO";

                                        urlBrokerPai = urlNovoBrokerAtivo.Key;

                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoBrokerAtivo.Key);
                                        try
                                        {
                                            delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                            funcaoCallBack = new AsyncCallback(OnExit3);
                                            IAsyncResult ar1 = del.BeginInvoke(urlBrokerAtivo.Key, urlNovoBrokerAtivo.Key, this.url, funcaoCallBack, null);
                                        }
                                        catch (Exception e) { }
                                    }
                                }
                                delRegistoSubscriber3 del1 = new delRegistoSubscriber3(reenviaRegistosubscricao);
                                funcaoCallBack = new AsyncCallback(OnExit10);
                                IAsyncResult ar = del1.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, nodeAEnviar, funcaoAInvocar, site, funcaoCallBack, null);

                            }
                            else if (nodeAEnviar.Equals("filho"))
                            {
                                lock (lockSentinela)
                                {
                                    if (!sentinela)
                                    {
                                        sentinela = true;

                                        BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));

                                        string urlBrokerSuspeito = broker.getBroker();
                                        broker.setPrincipalComoSuspeito(); //metemos o atual principal como suspeito

                                        string urlNovoPrincipal = broker.novoLider(urlBrokerSuspeito); //é atualizado o novo broker principal e é recebido o seu url

                                        IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), urlNovoPrincipal);
                                        try
                                        {
                                            delRegistoBroker del = new delRegistoBroker(brok.brokerDetetaSuspeito);
                                            funcaoCallBack = new AsyncCallback(OnExit3);
                                            IAsyncResult ar1 = del.BeginInvoke(urlBrokerSuspeito, urlNovoPrincipal, this.url, funcaoCallBack, null);
                                        }
                                        catch (Exception e) { }
                                    }

                                    delRegistoSubscriber3 del1 = new delRegistoSubscriber3(reenviaRegistosubscricao);
                                    funcaoCallBack = new AsyncCallback(OnExit10);
                                    IAsyncResult ar = del1.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, nodeAEnviar, funcaoAInvocar, site, funcaoCallBack, null);
                                }
                            }
                        }
                    }catch(Exception e) { }
                }
                else
                {
                }
            }
            
        }

        public void reenviaRegistosubscricao(string id, string url, string comando, Evento evento, string urlQuemEnviouEvento, string nodeAEnviar, string funcaoAInvocar, string site)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            IBroker objetoBroker;
        
            if (nodeAEnviar.Equals("pai"))
            {
                if (funcaoAInvocar.Equals("subscribe"))
                {
                    KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                    if (!urlBrokerAtivo.Key.Equals(urlQuemEnviouEvento))
                    {
                        try
                        {
                            objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);

                            delRegistoSubscriber2 del = new delRegistoSubscriber2(objetoBroker.registoSubscriber);
                            funcaoCallBack = new AsyncCallback(OnExit8);
                            IAsyncResult ar =del.BeginInvoke(id, url, comando, evento, this.url, funcaoCallBack, null);
                        }catch(Exception e) { }
                    }
                    else
                    {
                        lock(lockRegistoSubscriber)
                                listaEventosEsperaACKRegistoSubscriber.RemoveAll(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber()));
                    }
                }
                else //se for unsubscribe
                {
                    KeyValuePair<string, string> urlBrokerAtivo = listaBrokersPais.Single(x => x.Value.Equals("ATIVO")); //vamos buscar o broker que está atualmente ativo para torna-lo suspeito
                    if (!urlBrokerAtivo.Key.Equals(urlQuemEnviouEvento))
                    {
                        try
                        {
                            objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokerAtivo.Key);

                            delRegistoSubscriber2 del = new delRegistoSubscriber2(objetoBroker.unsubscribe);
                            funcaoCallBack = new AsyncCallback(OnExit8);
                            IAsyncResult ar =del.BeginInvoke(id, url, comando, evento, this.url, funcaoCallBack, null);
                        }catch(Exception e) { }
                    }
                    else
                    {
                        lock(lockRegistoSubscriber)
                            listaEventosEsperaACKRegistoSubscriber2.RemoveAll(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber()));
                    }
                }
            }
            else if (nodeAEnviar.Equals("filho"))
            {
                if (funcaoAInvocar.Equals("subscribe"))
                {

                    BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));
                    if (!broker.getBroker().Equals(urlQuemEnviouEvento))
                    {
                        try
                        {
                            objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), broker.getBroker());
                            delRegistoSubscriber2 del = new delRegistoSubscriber2(objetoBroker.registoSubscriber);
                            funcaoCallBack = new AsyncCallback(OnExit8);
                            IAsyncResult ar =del.BeginInvoke(id, url, comando, evento, this.url, funcaoCallBack, null);
                        }catch(Exception e) { }
                    }
                    else 
                    {
                        lock(lockRegistoSubscriber)
                            listaEventosEsperaACKRegistoSubscriber.RemoveAll(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber()));
                    }
                }
                else // se for unsubscribe
                {
                    BrokerFilho broker = listaFilhos.Single(x => x.getSite().Equals(site));
                    if (!broker.getBroker().Equals(urlQuemEnviouEvento))
                    {
                        try
                        {
                            objetoBroker = (IBroker)Activator.GetObject(typeof(IBroker), broker.getBroker());
                            delRegistoSubscriber2 del = new delRegistoSubscriber2(objetoBroker.unsubscribe);
                            funcaoCallBack = new AsyncCallback(OnExit8);
                            IAsyncResult ar =del.BeginInvoke(id, url, comando, evento, this.url, funcaoCallBack, null);
                        }catch(Exception e) { }
                    }
                    else
                    {
                        lock(lockRegistoSubscriber)
                            listaEventosEsperaACKRegistoSubscriber2.RemoveAll(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber()));
                    }
                }

            }
            Thread.Sleep(10000);

            lock (lockRegistoSubscriber)
            {
                if (funcaoAInvocar.Equals("subscribe"))
                {
                    if (listaEventosEsperaACKRegistoSubscriber.Any(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber())))
                    {
                        lock (lockSentinela)
                        {
                            if (!sentinela2)
                            {
                                sentinela = false;
                                sentinela3 = false;
                                sentinela2 = true;
                            }
                        }

                        delRegistoSubscriber3 del1 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                        funcaoCallBack = new AsyncCallback(OnExit10);
                        IAsyncResult ar =del1.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, "pai", "subscribe", site, funcaoCallBack, null);
                    }
                    else
                    {
                        lock (lockSentinela)
                            sentinela2 = false;
                    }
                }else if(funcaoAInvocar.Equals("unsubscribe"))
                {
                    if (listaEventosEsperaACKRegistoSubscriber2.Any(x => x.getIdentificador() == evento.getIdentificador() && x.getIdSubscriber().Equals(evento.getIdSubscriber())))
                    {
                        lock (lockSentinela)
                        {
                            if (!sentinela2)
                            {
                                sentinela = false;
                                sentinela3 = false;
                                sentinela2 = true;
                            }
                        }
                        delRegistoSubscriber3 del1 = new delRegistoSubscriber3(verificaEntregaCompletaRegistoSubscriber);
                        funcaoCallBack = new AsyncCallback(OnExit10);
                        IAsyncResult ar =del1.BeginInvoke(id, url, comando, evento, urlQuemEnviouEvento, "pai", "unsubscribe", site, funcaoCallBack, null);
                    }
                    else
                    {
                        lock (lockSentinela)
                            sentinela2 = false;
                    }
                }
            }
        }

        public void recebeACKRegistoSubscriber(int iden, string idSubscriber) //ACK enviado pelo broker
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            //como o evento foi enviado com sucesso, remove-lo da lista

            lock (lockRegistoSubscriber)
                listaEventosEsperaACKRegistoSubscriber.RemoveAll(x => x.getIdentificador() == iden && x.getIdSubscriber().Equals(idSubscriber));
        }

        public void recebeACKUnsubscribe(int iden, string idSubscriber)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            //como o evento foi enviado com sucesso, remove-lo da lista
            lock (lockRegistoSubscriber)
                listaEventosEsperaACKRegistoSubscriber2.RemoveAll(x => x.getIdentificador() == iden && x.getIdSubscriber().Equals(idSubscriber));
        }


        //método responsável por registar quais sao os filhos brokers
        public void registoBroker(string url, string estado, string site)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lockRegisto)
            {
                if (listaFilhos.Count == 0)
                {
                    BrokerFilho bro = new BrokerFilho(url, estado, site);
                    listaFilhos.Add(bro);
                }
                else
                {
                    if (listaFilhos.Any(x => x.getSite().Equals(site)))
                    {
                        BrokerFilho bro = listaFilhos.Find(x => x.getSite().Equals(site));
                        listaFilhos.Remove(bro);
                        bro.adicionaEntradaLista(url, estado);
                        listaFilhos.Add(bro);
                    }
                    else
                    {
                        BrokerFilho bro = new BrokerFilho(url, estado, site);
                        listaFilhos.Add(bro);
                    }
                }
            }

            BrokerFilho bro1 = listaFilhos.Find(x => x.getSite().Equals(site));

            if (bro1.getCountLista() == 3) //se já tiver os filhos todos, então envia para os seus brokers irmaos a lista de filhos
            {            
                lock(this)
                {
                    foreach (KeyValuePair<string, string> brokIrmao in listaIrmaos)
                    {
                        try
                        {
                            IBroker brok = (IBroker)Activator.GetObject(typeof(IBroker), brokIrmao.Key);
                            delATualizaListaFilhos del = new delATualizaListaFilhos(brok.AtualizaListaFilhos);
                            funcaoCallBack = new AsyncCallback(OnExit2);
                            IAsyncResult ar =del.BeginInvoke(listaFilhos, funcaoCallBack, null);
                        }
                        catch(Exception e) { }
                    }
                }
            }        
        }

        public void AtualizaListaFilhos(List<BrokerFilho> listaFilhos)
        {
            this.listaFilhos = listaFilhos;
        }

        public void criarTabelaEncaminhamento(string urlPai, int numeroBrokers)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            //este while é executado até a tabela de encaminhamento ter sido preenchida
            while (!(completo == numeroBrokers - 1))
            {              
                lock (this)
                {
                    for (int i = 0; i < listaFilhos.Count; i++) //atualizamos a tabela do pai conforme os filhos deste nós e subfilhos
                    {
                        BrokerFilho brok = listaFilhos[i];
                        try
                        {
                            IBroker bro = (IBroker)Activator.GetObject(typeof(IBroker), brok.getBroker());

                            if (!tabelaEncaminhamento.ContainsKey(bro.getID())) //para os casos em que a iteração do while é repetida, para nao voltar a adicionar
                            {
                                tabelaEncaminhamento.Add(bro.getID(), bro.getUrl());
                                completo++;
                            }
                            tabelaRecursiva(bro, bro.getUrl());
                        }
                        catch(Exception e) { }
                    }
                }
                if (!urlPai.Equals("raiz")) //Se for a raiz nao vamos buscar o pai. Este if vai buscar a tabela de encaminhamento do pai e atualiza a tabela deste broker conforme essa
                {
                    IBroker pai = (IBroker)Activator.GetObject(typeof(IBroker), urlPai);
                    try {
                        Dictionary<string, string> tabelaPai = pai.getTabelaEncaminhamento();

                        foreach (string id in tabelaPai.Keys) //vamos iterar sobre os ids e adicionamos a esta tabela, todas os ids que esta nao contenha com o url do pai que é a maneira de ter acesso a esses brokers 
                        {
                            if (!tabelaEncaminhamento.ContainsKey(id) && !id.Equals(this.id)) //se nao contiver um caminha para este id (do broker), adicionamo-lo. A segunda condiçao é para nao adicionar a si proprio na tabela
                            {
                                tabelaEncaminhamento.Add(id, urlPai);
                                completo++;
                            }
                        }

                        if (!tabelaEncaminhamento.ContainsKey(pai.getID()))
                        {
                            tabelaEncaminhamento.Add(pai.getID(), urlPai); // adicionamos o pai à tabela de encaminhamento
                            completo++;
                        }
                    }
                    catch(Exception e) { }
                }
            }

            lock (lockEncaminhamento)
            {
                podeReceberEventos = true; // a tabela de encaminhamento está concluida portanto podemos receber eventos
                Monitor.PulseAll(lockEncaminhamento); //libertamos os eventos bloqueados
            }

            //após a tabela de encaminhamento estar criada, passamo-la para os seus irmaos
            foreach (KeyValuePair<string, string> urlBrokIrmao in listaIrmaos) 
            {
                try
                {
                    IBroker brokIrmao = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokIrmao.Key);
                    delTabelaEncaminhamento del = new delTabelaEncaminhamento(brokIrmao.adicionaTabelaEncaminhamento);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(tabelaEncaminhamento, funcaoCallBack, null);
                }
                catch(Exception e) { }
            }
        }

        //função responsável por percorrer a árvore toda para adicionar os filhos do filho que invoca esta função à sua tabela de encaminhamento
        public void tabelaRecursiva(IBroker bro, string url)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }

            for (int i = 0; i < bro.getListaFilhos().Count; i++)
            {
                try
                {
                    BrokerFilho brok = bro.getListaFilhos()[i];
                    IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), brok.getBroker());
                    if (!tabelaEncaminhamento.ContainsKey(broFilho.getID()))
                    {
                        tabelaEncaminhamento.Add(broFilho.getID(), url);
                        completo++;
                    }
                    delTabela del = new delTabela(tabelaRecursiva); //voltar a invocar a tabela recursivamente para atingir todos os nós, no caso se haver mais filhos
                    funcaoCallBack = new AsyncCallback(OnExit1);
                    IAsyncResult ar =del.BeginInvoke(broFilho, url, funcaoCallBack, null);
                }
                catch(Exception e) { }
            }
        }

        public void atualizaTabelaEncaminhamento(string idBroker, string urlBroker, string urlBrokerSuspeito)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lockSentinela)
            {
                sentinela = true;
                sentinela4 = true;
                sentinela2 = true;
                sentinela3 = true;
            }
            for (int i = 0; i < tabelaEncaminhamento.Count; i++)
            {
                KeyValuePair<string, string> item = tabelaEncaminhamento.ElementAt(i);

                if (item.Value.Equals(urlBrokerSuspeito)) // atualizamos o novo broker a quem devem enviar
                {
                    tabelaEncaminhamento[item.Key] = urlBroker;
                }
            }

            foreach (BrokerFilho bro in listaFilhos) //é necessário propagar a tua atualização para os filhos, 
            {
                try
                {
                    IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), bro.getBroker());
                    if (broFilho.getUrl().Equals(urlBroker))
                        continue;

                }catch(Exception e) { }

                if (routingPolicy.Equals("filtering")) //se a routing policy for filtering, entao temos que atualizar a tabela de encaminhamento do broker
                {
                    try
                    {
                        IBroker broFilho = (IBroker)Activator.GetObject(typeof(IBroker), bro.getBroker());

                        delRegistoBroker del = new delRegistoBroker(broFilho.atualizaTabelaEncaminhamento);
                        funcaoCallBack = new AsyncCallback(OnExit7);
                        IAsyncResult ar =del.BeginInvoke(id, urlBroker, urlBrokerSuspeito, funcaoCallBack, null);
                    }
                    catch(Exception e) { }
                }
            }            

            //enviamos a atualizacao da tabela para os irmaos
            foreach (KeyValuePair<string, string> urlBrokIrmao in listaIrmaos)
            {
                try
                {
                    IBroker brokIrmao = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokIrmao.Key);
                    delTabelaEncaminhamento del = new delTabelaEncaminhamento(brokIrmao.adicionaTabelaEncaminhamento);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(tabelaEncaminhamento, funcaoCallBack, null);
                }catch(Exception e) { }
            }
        }

        public List<BrokerFilho> getListaFilhos() //vamos buscar os filhos de bro
        {
            return listaFilhos;
        }

        public Dictionary<string, string> getTabelaEncaminhamento()
        {
            return tabelaEncaminhamento;
        }

        public void verificaListaEsperaPropagacao()
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_4)
            {
                foreach (Evento evento in listaPropagacaoEspera)
                {
                    IBroker brokerQueEnviou = (IBroker)Activator.GetObject(typeof(IBroker), evento.getUrlBrokerEnviou());
                    Site siteBrokerEnviou = listaSites2.Find(x => x.getSite().Equals(brokerQueEnviou.getSite()));

                    if (evento.getSeqNumberSite() == siteBrokerEnviou.getSeqNumber() + 1)   //Se for o evento que estávamos à espera
                    {
                        if (tabelaEncaminhamento.ContainsKey(evento.getIdBrokerFinal())) //vemos se a tabela contém este broker, se contiver entao reencaminhamos para o broker seguinte até chegarmos ao destino
                        {
                            lock (lockPropagacao)
                                listaEventosEsperaACKPropagacaoEvento.Add(evento);

                            delVerificaPropragacao del1 = new delVerificaPropragacao(verificaEntregaCompletaPropagacao);
                            AsyncCallback funcaoCallBack = new AsyncCallback(OnExit13);
                            IAsyncResult ar =del1.BeginInvoke(evento, evento.getIdBrokerFinal(), evento.getUrlSubscriber(), evento.getIdSubscriber(), evento.getSubscricao(), url, "FIFO", funcaoCallBack, null);

                            IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), tabelaEncaminhamento[evento.getIdBrokerFinal()]);
                            try
                            {
                                if (!listaSites.Exists(x => x.getSite().Equals(broker.getSite())))  //Se for a primeira vez que estamos a mandar para este site um evento
                                {
                                    Site sitenovo = new Site(broker.getSite(), 0);
                                    listaSites.Add(sitenovo);
                                }

                                Site site = listaSites.Find(x => x.getSite().Equals(broker.getSite()));

                                site.incrementSeqNumber();
                                siteBrokerEnviou.incrementSeqNumber();
                                evento.setSeqNumberSite(site.getSeqNumber());
                                listaPropagacaoEspera.Remove(evento);

                                delPropagacaoFIFO del = new delPropagacaoFIFO(broker.propagacaoEventoFIFO);
                                funcaoCallBack = new AsyncCallback(OnExitPropagacaoFIFO);
                                IAsyncResult ar1 =del.BeginInvoke(evento, evento.getIdBrokerFinal(), evento.getUrlSubscriber(), evento.getIdSubscriber(), evento.getSubscricao(), getUrl(), funcaoCallBack, null);

                                verificaListaEsperaPropagacao();
                            }catch(Exception e) { }
                        }
                        else        //Se já estamos no broker que tem o subscriber correspondente
                        {
                            ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), evento.getUrlSubscriber());
                            sub.recebeEvento(evento, true);

                            listaPropagacaoEspera.Remove(evento);
                            siteBrokerEnviou.incrementSeqNumber();
                            verificaListaEsperaPropagacao();
                        }

                    }
                }
            }
        }

        public void verificaEsperaFiltering()
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lockEventosEspera)
            {
                for(int i = 0; i<listaEventosEspera.Count; i++)
                {
                    Evento evento = listaEventosEspera[i];

                    if (evento.getSeqNumberSite() == listaPublishersLocais[evento.getNomePublisher()] + 1)
                    {
                        foreach (Subscricao subscricao in tabelaSubscricoes)
                            enviaSubscriberFilteringFIFO(evento, subscricao);

                        listaEventosEspera.Remove(evento);
                        listaPublishersLocais[evento.getNomePublisher()]++;
                        verificaEsperaFiltering();


                        delConfirmacao del = new delConfirmacao(verificaEsperaFiltering);
                        funcaoCallBack = new AsyncCallback(OnExit14);
                        IAsyncResult ar =del.BeginInvoke(funcaoCallBack, null);

                        //verificaEsperaFiltering();
                        break;
                    }
                }

            }

        }

        public void verificaEsperaFlooding()
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            foreach (Evento evento in listaEventosEspera)
            {
                if (evento.getSeqNumberSite() == seqNumberSite + 1)
                {

                    foreach (Subscricao subscricao in tabelaSubscricoes)
                    {
                        string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
                        if (topicos.Last().Equals("*"))
                        {
                            int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                            string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                            if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                                EnviaSubscriberSincrono(evento, subscricao);
                        }
                        else if (subscricao.getEvento().getTopic().Equals(evento.getTopic())) //se der true, entao este subscritor está subscrito a este evento e recebe-o
                            EnviaSubscriberSincrono(evento, subscricao);
                    }
                    seqNumberSite++;
                    lock(lockEventosEspera)
                        listaEventosEspera.Remove(evento);
                    delConfirmacao del = new delConfirmacao(verificaEsperaFlooding);
                    funcaoCallBack = new AsyncCallback(OnExit14);
                    IAsyncResult ar =del.BeginInvoke(funcaoCallBack, null);
                   // verificaEsperaFlooding();
                    break;
                }
            }
        }

        public void setOrderingPolicy(string policy)
        {
            orderingPolicy = policy;
        }

        public string getUrl()
        {
            return url;
        }

        public void atualizaURLPai(string urlPaiNovo)
        {
            urlBrokerPai = urlPaiNovo;
        }

        public string getEstado()
        {
            return estado;
        }

        //função invocada pelos irmãos quando o principal envia uma atualização de uma subscricao
        public void atualizaListaSubscriber(Subscricao subs, string urlQuemEnviouEvento)
        {
            tabelaSubscricoes.Add(subs);

            List<Subscricao> tabelaAux = new List<Subscricao>();

            //remover duplicados
            for (int i = 0; i < tabelaSubscricoes.Count; i++)
            {
                if (tabelaAux.Count == 0)
                    tabelaAux.Add(tabelaSubscricoes[i]);

                if (!tabelaAux.Any(x => x.getEvento().getTopic().Equals(tabelaSubscricoes[i].getEvento().getTopic()) && x.getIDSubscriber().Equals(tabelaSubscricoes[i].getIDSubscriber())))
                {
                    tabelaAux.Add(tabelaSubscricoes[i]);
                }
            }
            tabelaSubscricoes = tabelaAux;

            //atualiza os seus irmaos 
        }

        //funcao invocada pelos irmaos quando o principal envia uma atualização de um unsubscribe
        public void atualizaListaSubscriber2(Evento evento, string urlQuemEnviouEvento)
        {
            tabelaSubscricoes.Remove(tabelaSubscricoes.Find(x => x.getEvento().getTopic().Equals(evento.getTopic())&& x.getIDSubscriber().Equals(evento.getIdSubscriber())));
            //atualiza os seus irmaos do unsubscribe 

            if(!listaIrmaos.Any(x => x.Key.Equals(urlQuemEnviouEvento)))
                foreach (KeyValuePair<string, string> urlBrokIrmao in listaIrmaos) //vamos avisar os irmaos que houve um unsubscribe
                {
                    try
                    {
                        IBroker brokIrmao = (IBroker)Activator.GetObject(typeof(IBroker), urlBrokIrmao.Key);
                        delRemove del = new delRemove(brokIrmao.atualizaListaSubscriber2);
                        AsyncCallback funcaoCallBack = new AsyncCallback(OnExit17);
                        IAsyncResult ar =del.BeginInvoke(evento, this.url, funcaoCallBack, null);
                    }catch(Exception e) { }
                }                           
        }

        public void enviaSubscriberFloodingSincrono(Evento evento, Subscricao subscricao)       //Para ser usado no Flooding - TOTAL e FIFO - Se não for sincrono, o subscriber pode receber eventos fora de ordem
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());

            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
            if (topicos.Last().Equals("*"))
            {
                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                {
                    sub.recebeEvento(evento, true);
                }
            }
            else if (subscricao.getEvento().getTopic().Equals(evento.getTopic())) //se der true, entao este subscritor está subscrito a este evento e recebe-o
            {
                sub.recebeEvento(evento, true);
            }
        }

        public void enviaSubscriberFloodingAssincrono(Evento evento, Subscricao subscricao)       //Para ser usado no Flooding - TOTAL e FIFO - Se não for sincrono, o subscriber pode receber eventos fora de ordem
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            ISubscriber sub = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());

            string[] topicos = subscricao.getEvento().getTopic().Split('/'); //separamos os topicos para sabermos se há um *
            if (topicos.Last().Equals("*"))
            {
                int index = topicos.Count() - 1; //vamos buscar o que está antes de /*, por exemplo /desporto/futebol/*, vai buscar futebol
                string[] topicosEventosRecebido = evento.getTopic().Split('/'); //tambem temos que separar o evento recebido para ver se o penultimo coincide com o penultimo do que tem o *

                if (topicos[index - 1].Equals(topicosEventosRecebido[index - 1]))
                {
                    delTrataSubscriber del = new delTrataSubscriber(sub.recebeEvento);
                    funcaoCallBack = new AsyncCallback(OnExit);
                    IAsyncResult ar =del.BeginInvoke(evento, true, funcaoCallBack, null);
                }
            }
            else if (subscricao.getEvento().getTopic().Equals(evento.getTopic())) //se der true, entao este subscritor está subscrito a este evento e recebe-o
            {
                delTrataSubscriber del = new delTrataSubscriber(sub.recebeEvento);
                funcaoCallBack = new AsyncCallback(OnExit);
                IAsyncResult ar =del.BeginInvoke(evento, true, funcaoCallBack, null);
            }
        }

        public void EnviaSubscriberSincrono(Evento evento, Subscricao subscricao)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            ISubscriber sub1 = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());
            sub1.recebeEvento(evento, true);
        }

        public void EnviaSubscriberAssincrono(Evento evento, string urlSubscriber)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del1 = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar1 =del1.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            ISubscriber sub1 = (ISubscriber)Activator.GetObject(typeof(ISubscriber), urlSubscriber);
            delTrataSubscriber del = new delTrataSubscriber(sub1.recebeEvento);
            funcaoCallBack = new AsyncCallback(OnExit);
            IAsyncResult ar =del.BeginInvoke(evento, true, funcaoCallBack, null);
        }

        public void EnviaSubscriberAssincrono(Evento evento, Subscricao subscricao)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del1 = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar1 =del1.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            ISubscriber sub1 = (ISubscriber)Activator.GetObject(typeof(ISubscriber), subscricao.getURL());
            delTrataSubscriber del = new delTrataSubscriber(sub1.recebeEvento);
            funcaoCallBack = new AsyncCallback(OnExit);
            IAsyncResult ar =del.BeginInvoke(evento, true, funcaoCallBack, null);
        }

        public void EnviaSubscriberSincrono(Evento evento, string urlSubscriber)
        {
            ISubscriber sub1 = (ISubscriber)Activator.GetObject(typeof(ISubscriber), urlSubscriber);
            sub1.recebeEvento(evento, true);
        }

        public string getSite()
        {
            return site;
        }

        public string getUrlPai()
        {
            return urlBrokerPai;
        }

        public string getID()
        {
            return id;
        }

        public string getIDOriginal()
        {
            return idBrokerPrincipalOriginal;
        }

        public Dictionary<string, string> getListaIrmaos()
        {
            lock(this)
                return listaIrmaos;
        }

        public void adicionaTabelaEncaminhamento(Dictionary<string, string> tabela)
        {          
            tabelaEncaminhamento = tabela;
            podeReceberEventos = true;
        }

        public void setRoutingPolicy(string policy, bool criaTabela)
        {
            routingPolicy = policy;
            /*
                        if (policy.Equals("filtering") && criaTabela) //só o broker principal cria a tabela de encaminhamento, quando esta está completa, ele passa-a para os seus irmaos.
                        {
                            criarTabelaEncaminhamento(urlBrokerPai, numeroBrokers); //criamos a tabela de encaminhamento
                        }*/

            if ( criaTabela) //só o broker principal cria a tabela de encaminhamento, quando esta está completa, ele passa-a para os seus irmaos.
            {
                criarTabelaEncaminhamento(urlBrokerPai, numeroBrokers); //criamos a tabela de encaminhamento
            }
            

        }

        public void recebeACKPedidoSequencer(int iden, string site, string nomePublisher)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock(lock_3)
                listaPedidoEspera.RemoveAll(x => x.getSeqNumber() == iden && x.getSite().Equals(site) && x.getNomePublisher().Equals(nomePublisher));
        }

        public void recebeACKRecebeEventoTotal(int iden, string site, string nomePublisher)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock (lock_2)
                listaEsperaTotalACK.RemoveAll(x => x.getSeqNumber() == iden && x.getSite().Equals(site) && x.getNomePublisher().Equals(nomePublisher));
        }

        public void recebeACKPropagacaoTotal(int iden, string site, string nomePublisher)
        {
            lock (this)
            {
                if (freezeAtivo)
                {
                    Monitor.Wait(this);
                    if (estado.Equals("SUSPEITO")) //se o estado for suspeito, descartamos estes evento
                        return;
                    else if (estado.Equals("MORTO")) //se passou demasiado tempo, ele foi declarado morto quando na realidade apenas estava lento. se ele foi declarado morto, então envia pings para os seus irmaos para avisar que afinal nao está morto
                    {
                        KeyValuePair<string, string> irmaoAtivo = listaIrmaos.First(x => x.Value.Equals("ATIVO"));//vamos buscar o irmao atualmente ativo e avisa-lo que este broker afinal nao está morto. O broker ativo avisa todos o resto da rede que este broker afinal nao estava morto
                        IBroker brokAtivo = (IBroker)Activator.GetObject(typeof(IBroker), irmaoAtivo.Key);

                        delBrokerSuspeito del = new delBrokerSuspeito(brokAtivo.estouVivo);
                        funcaoCallBack = new AsyncCallback(OnExit4);
                        IAsyncResult ar =del.BeginInvoke(this.url, funcaoCallBack, null);
                        return;
                    }
                }
            }
            lock(lockTotal)
                listaEsperaPropagacaoTotalACK.RemoveAll(x => x.getSeqNumber() == iden && x.getSite().Equals(site) && x.getNomePublisher().Equals(nomePublisher));


        }

        public string getRoutingPolicy()
        {
            return routingPolicy;
        }

        static public void OnExit(IAsyncResult ar)
        {
            delTrata del = (delTrata)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit1(IAsyncResult ar)
        {
            delTabela del = (delTabela)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit2(IAsyncResult ar)
        {
            delATualizaListaFilhos del = (delATualizaListaFilhos)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit3(IAsyncResult ar)
        {
            delFault del = (delFault)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        public void OnExit4(IAsyncResult ar)
        {
            delAtualiza del = (delAtualiza)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit5(IAsyncResult ar)
        {
            delAtualizaLider del = (delAtualizaLider)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit6(IAsyncResult ar)
        {
            delRegistoSubscriber del = (delRegistoSubscriber)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit7(IAsyncResult ar)
        {
            delRegistoBroker del = (delRegistoBroker)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit8(IAsyncResult ar)
        {
            delRegistoSubscriber2 del = (delRegistoSubscriber2)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit9(IAsyncResult ar)
        {
            delPropagacao del = (delPropagacao)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit10(IAsyncResult ar)
        {
            lock (lockEvento)
            {
                lockEventos = false;
                Monitor.PulseAll(lockEvento);
            }
            delRegistoSubscriber3 del = (delRegistoSubscriber3)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit11(IAsyncResult ar)
        {
            delEnviaFiltering del = (delEnviaFiltering)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit12(IAsyncResult ar)
        {
            delEnviaBroker del = (delEnviaBroker)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
        static public void OnExit13(IAsyncResult ar)
        {
            delVerificaPropragacao del = (delVerificaPropragacao)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit14(IAsyncResult ar)
        {
            delConfirmacao del = (delConfirmacao)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit15(IAsyncResult ar)
        {
            delTotal del = (delTotal)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit16(IAsyncResult ar)
        {
            delPedido del = (delPedido)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExit17(IAsyncResult ar)
        {
            delRemove del = (delRemove)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        public void OnExit90(IAsyncResult ar)
        {
            lock(lock_)
            {
                Monitor.PulseAll(lock_);
                ajudaLock = false;
            }
            lock(lockSentinela)
                 sentinela4 = false;
            

            delRegistoBroker del = (delRegistoBroker)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }
        public void OnExit44(IAsyncResult ar)
        {
            lock (lock_)
            {
                Monitor.PulseAll(lock_);// depois de estar tudo atualizado, o processamento pode recomecar
                ajudaLock = false;
            }
            delAtualiza del = (delAtualiza)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExitSequencer(IAsyncResult ar)
        {
            delSequencer del = (delSequencer)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExitRecebeTotal(IAsyncResult ar)
        {
            delRecebeTotal del = (delRecebeTotal)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

        static public void OnExitPropagacaoFIFO(IAsyncResult ar)
        {
            delPropagacaoFIFO del = (delPropagacaoFIFO)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke(ar);
        }

    }
}

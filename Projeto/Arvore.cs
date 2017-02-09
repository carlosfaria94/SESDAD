using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


/*
    adaptado de: http://www.codeproject.com/Articles/345191/Simple-Generic-Tree
*/
namespace Projeto
{
    //esta classe define o código para implementação da árvore de como está organizada a rede

    //Classe base da arvore genérica
    public abstract class TreeNodeBase<T> : ITreeNode<T> where T : class, ITreeNode<T>
    {

        protected TreeNodeBase(string name)
        {
            Name = name;
            ChildNodes = new List<T>();
        }

        public string Name
        {
            get;
            private set;
        }

        public T Parent
        {
            get;
            set;
        }

        public List<T> ChildNodes
        {
            get;
            private set;
        }

        public List<string> listaIrmaos = new List<string>();
    

        protected abstract T MySelf
        {
            get;
        }

        //true se for uma nó folha
        public bool IsLeaf
        {
            get { return ChildNodes.Count == 0; }
        }

        //true se for a raiz(site0)
        public bool IsRoot
        {
            get { return Parent == null; }
        }

        public List<T> getFilhos() //obter o nome dos filhos
        {
            return ChildNodes;
        }

        //lista dos nós folha
        public List<T> GetLeafNodes()
        {
            return ChildNodes.Where(x => x.IsLeaf).ToList();
        }

        //lista dos nós nao folha
        public List<T> GetNonLeafNodes()
        {
            return ChildNodes.Where(x => !x.IsLeaf).ToList();
        }

        //obter o root da arvore
        public T GetRootNode()
        {
            if (Parent == null)
                return MySelf;

            return Parent.GetRootNode();
        }

        //adicionar um filho
        public void AddChild(T child)
        {
            child.Parent = MySelf; //quem invoca esta função é o pai, daí o child.Parent = MySelf
            ChildNodes.Add(child); //adicionamos o nó proveniente do argumento à lista de filhos
        }

        //adicionar um broker irmao
        public void addIrmao(string urlIrmao)
        {

            listaIrmaos.Add(urlIrmao);
        }

        //obter os outros brokers do mesmo site
        public List<string> getIrmaos()
        {
            return listaIrmaos;
        }
    
    }

    public class Arvore : TreeNodeBase<Arvore>
    {
        public Arvore(string name)
            : base(name)
        {
        }

        protected override Arvore MySelf
        {
            get { return this; }
        }   
    }


    public interface ITreeNode<T>
    {
        T Parent { get; set; }
        string Name { get;  }
        bool IsLeaf { get; }
        bool IsRoot { get; }
        List<T> getFilhos();
        T GetRootNode();
        List<string> getIrmaos();
    }

}
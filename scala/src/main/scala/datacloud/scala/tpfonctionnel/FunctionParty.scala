package datacloud.scala.tpfonctionnel

object FunctionParty {
    def curryfie[A, B, C](f: (A, B) => C): A => B => C = {
        val curriedF: A => B => C = a => b => f(a, b)
        return curriedF
    }

    def decurryfie[A, B, C](f: A => B => C): (A, B) => C = {
        val decurriedF: (A, B) => C = (a, b) => f(a)(b)
        return decurriedF
    }

    def compose[A, B, C](f: B => C, g: A => B): A => C = {
        def comp = (a: A) => f(g(a))

        return comp;
    }

    def axplusb(a: Int, b: Int): Int => Int = {
        def add = (x: Int, y: Int) => x + y

        def mult = (x: Int, y: Int) => x * y

        var f1 = (x: Int) => curryfie(add)(x);
        var f2 = (x: Int) => curryfie(mult)(x);
        return (x: Int) => compose(f1(b), f2(a))(x);
    }
}

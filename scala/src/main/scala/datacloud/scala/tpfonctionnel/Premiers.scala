package datacloud.scala.tpfonctionnel

object Premiers {
    def premiers(n: Int):List[Int] = {
        var prem = List.range(2,n)
        for (i <- 2 to n) {
            prem = prem.filter((a: Int) => a % i != 0 || a == i)
        }
        prem
    }

    def premiersWithRec(n: Int):List[Int] = {
        val prem = List.range(2, n)
        def rec(l: List[Int]): List[Int] = {
            if (l.head * l.head > l.last)
                l
            else {
                List(l.head) ++ rec((l.filter(_ % l.head != 0)))
            }
        }
        rec(prem)
    }
}

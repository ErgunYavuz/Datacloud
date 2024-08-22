package revision

case class edge[+T](source:T, dest:T) {
    def reverse():edge[T] = edge(dest,source)
    def toCouple:(T,T) = (source, dest)
    var i = 3
    protected implicit var n
}

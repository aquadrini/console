package zookeeper


interface DataListener<T> {

    void process(T data)

}

package jobs


interface DataListener<T> {

    void process(T data)

}

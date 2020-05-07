package com.daxiang.digest.framework;



import java.util.List;

public interface Execution<SR extends Source, TF extends Transform, SK extends Sink> extends Plugin<Void> {
    void start(List<SR> sources, List<TF> transforms, List<SK> sinks);
}

package es.bsc.mobile.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CoreElement {

    public JavaMethod[] methods();

    public OpenCL[] openclKernels() default {};

    public Service[] services() default {};
}

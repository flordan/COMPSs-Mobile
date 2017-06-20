package es.bsc.mobile.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface JavaMethod {

    /**
     * Set of classes implementing the method.
     *
     * @return an array of Strings containing the names of all the classes
     * implementing the method.
     */
    String declaringClass();

    Constraints constraints() default @Constraints();
}

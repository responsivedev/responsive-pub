FROM public.ecr.aws/j8q9y0n6/responsivedev/system-test-base:0.1.0

COPY libs/*.jar /usr/share/java/responsive-e2etest/
COPY scripts/* /

CMD /run-e2etest

__kernel void vecMultiplication(__global const float *a,
                           __global const float *b,
                           __global float *c)
{
    int gid = get_global_id(0);
    c[gid] = a[gid] * b[gid];
}

__kernel void scalarMultiplication(const float a,
                           __global float *b)
{
    int gid = get_global_id(0);
	for (int i=0;i<10000;i++){
		for (int j=0;j<10000;j++) {
		   	 b[gid] = a * b[gid];
		   	 b[gid] = b[gid]/a;
		}
	}

   	b[gid] = a * b[gid];
}

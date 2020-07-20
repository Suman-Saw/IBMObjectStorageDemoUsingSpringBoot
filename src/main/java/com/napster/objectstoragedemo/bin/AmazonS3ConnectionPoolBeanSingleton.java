package com.napster.objectstoragedemo.bin;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author ArnabBiswas
 *
 */

@Configuration
@Service
public class AmazonS3ConnectionPoolBeanSingleton {

	@Bean
	public synchronized AmazonS3ConnectionPoolBean prototypeBean() {
		return new AmazonS3ConnectionPoolBean();
	}

}

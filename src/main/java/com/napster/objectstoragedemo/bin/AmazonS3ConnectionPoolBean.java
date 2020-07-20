package com.napster.objectstoragedemo.bin;

import com.amazonaws.services.s3.AmazonS3;
import lombok.Data;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

/**
 * @author ArnabBiswas
 *
 */

@Data
@Configuration
@Service
public class AmazonS3ConnectionPoolBean {	
	private AmazonS3 s3ClientBean;
}

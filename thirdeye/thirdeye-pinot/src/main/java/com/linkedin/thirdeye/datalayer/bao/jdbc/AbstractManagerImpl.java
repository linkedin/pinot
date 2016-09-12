package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.modelmapper.ModelMapper;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.datalayer.bao.AbstractManager;
import com.linkedin.thirdeye.datalayer.dao.GenericPojoDao;
//import com.linkedin.thirdeye.datalayer.dao.AnomalyFeedbackDAO;
//import com.linkedin.thirdeye.datalayer.dao.AnomalyFunctionDAO;
//import com.linkedin.thirdeye.datalayer.dao.RawAnomalyResultDAO;
import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.JobBean;
import com.linkedin.thirdeye.datalayer.pojo.TaskBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public abstract class AbstractManagerImpl<E extends AbstractDTO> implements AbstractManager<E> {

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	protected static final ModelMapper MODEL_MAPPER = new ModelMapper();
	protected static GenericPojoDao genericPojoDao = DaoProviderUtil.getInstance(GenericPojoDao.class);

	Class<? extends AbstractDTO> dtoClass;
	Class<? extends AbstractBean> beanClass;

	protected AbstractManagerImpl(Class<? extends AbstractDTO> dtoClass, Class<? extends AbstractBean> beanClass) {
		this.dtoClass = dtoClass;
		this.beanClass = beanClass;
	}

	@Override
	public Long save(E entity) {
		AbstractBean bean = MODEL_MAPPER.map(entity, beanClass);
		return genericPojoDao.put(bean);
	}

	@Override
	public void update(E entity) {
		AbstractBean bean = MODEL_MAPPER.map(entity, beanClass);
		genericPojoDao.update(bean);
	}

	public E findById(Long id) {
		AbstractBean abstractBean = genericPojoDao.get(id, beanClass);
		AbstractDTO abstractDTO = MODEL_MAPPER.map(abstractBean, dtoClass);
		return (E) abstractDTO;
	}

	@Override
	public void delete(E entity) {
		genericPojoDao.delete(entity.getId(), beanClass);
	}

	@Override
	public void deleteById(Long id) {
		genericPojoDao.delete(id, beanClass);
	}

	@Override
	public List<E> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<E> findByParams(Map<String, Object> filters) {
		// TODO Auto-generated method stub
		return null;
	}

	protected AnomalyFunctionDTO fetchAnomalyFunction(Long anomalyFunctionId) throws Exception {
		AnomalyFunctionBean anomalyFunctionBean = genericPojoDao.get(anomalyFunctionId, AnomalyFunctionBean.class);
		AnomalyFunctionDTO anomalyFunctionDTO = new AnomalyFunctionDTO();
		MODEL_MAPPER.map(anomalyFunctionDTO, anomalyFunctionBean);
		return anomalyFunctionDTO;
	}

	protected AnomalyFeedbackDTO fetchAnomalyFeedback(Long feedbackId) throws Exception {
		AnomalyFeedbackBean anomalyFeedbackBean = genericPojoDao.get(feedbackId, AnomalyFeedbackBean.class);
		AnomalyFeedbackDTO anomalyFeedbackDTO = new AnomalyFeedbackDTO();
		MODEL_MAPPER.map(anomalyFeedbackDTO, anomalyFeedbackBean);
		return anomalyFeedbackDTO;
	}

}

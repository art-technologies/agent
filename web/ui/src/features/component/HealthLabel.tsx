import { FC } from 'react';
import styles from './HealthLabel.module.css';
import { ComponentHealthState } from './types';

interface HealthLabelProps {
  health: ComponentHealthState;
}

export const HealthLabel: FC<HealthLabelProps> = ({ health }) => {
  const healthMappings = {
    [ComponentHealthState.HEALTHY]: `${styles.health} ${styles['state-ok']}`,
    [ComponentHealthState.UNHEALTHY]: `${styles.health} ${styles['state-error']}`,
    [ComponentHealthState.UNKNOWN]: `${styles.health} ${styles['state-warn']}`,
    [ComponentHealthState.EXITED]: `${styles.health} ${styles['state-error']}`,
  };
  const healthClass = healthMappings[health];

  return <span className={healthClass}>{health}</span>;
};
